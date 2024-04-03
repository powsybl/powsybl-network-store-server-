/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.powsybl.commons.PowsyblException;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.iidm.network.ReactiveLimitsKind;
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.model.utils.VariantUtils;
import com.powsybl.network.store.server.dto.LimitsInfos;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import com.powsybl.network.store.server.exceptions.JsonApiErrorResponseException;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import com.powsybl.ws.commons.LogUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.powsybl.network.store.server.Mappings.*;
import static com.powsybl.network.store.server.QueryCatalog.*;
import static com.powsybl.network.store.server.Utils.bindAttributes;
import static com.powsybl.network.store.server.Utils.bindValues;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@Repository
public class NetworkStoreRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkStoreRepository.class);

    @Autowired
    public NetworkStoreRepository(DataSource dataSource, ObjectMapper mapper, Mappings mappings, ExtensionHandler extensionHandler) {
        this.dataSource = dataSource;
        this.mappings = mappings;
        this.mapper = mapper.registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
        this.extensionHandler = extensionHandler;
    }

    private final DataSource dataSource;

    private final ObjectMapper mapper;

    private final Mappings mappings;

    private final ExtensionHandler extensionHandler;

    static final int BATCH_SIZE = 1000;

    private static final String SUBSTATION_ID = "substationid";

    // network

    /**
     * Get all networks infos.
     */
    public List<NetworkInfos> getNetworksInfos() {
        try (var connection = dataSource.getConnection()) {
            try (var stmt = connection.createStatement()) {
                try (ResultSet resultSet = stmt.executeQuery(QueryCatalog.buildGetNetworkInfos())) {
                    List<NetworkInfos> networksInfos = new ArrayList<>();
                    while (resultSet.next()) {
                        networksInfos.add(new NetworkInfos(resultSet.getObject(1, UUID.class),
                                                           resultSet.getString(2)));
                    }
                    return networksInfos;
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public List<VariantInfos> getVariantsInfos(UUID networkUuid) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetVariantsInfos())) {
                preparedStmt.setObject(1, networkUuid);
                try (ResultSet resultSet = preparedStmt.executeQuery()) {
                    List<VariantInfos> variantsInfos = new ArrayList<>();
                    while (resultSet.next()) {
                        variantsInfos.add(new VariantInfos(resultSet.getString(1), resultSet.getInt(2)));
                    }
                    return variantsInfos;
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Optional<Resource<NetworkAttributes>> getNetwork(UUID uuid, int variantNum) {
        var networkMapping = mappings.getNetworkMappings();
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetNetworkQuery(networkMapping.getColumnsMapping().keySet()))) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.setInt(2, variantNum);
                try (ResultSet resultSet = preparedStmt.executeQuery()) {
                    if (resultSet.next()) {
                        NetworkAttributes attributes = new NetworkAttributes();
                        MutableInt columnIndex = new MutableInt(2);
                        networkMapping.getColumnsMapping().forEach((columnName, columnMapping) -> {
                            bindAttributes(resultSet, columnIndex.getValue(), columnMapping, attributes, mapper);
                            columnIndex.increment();
                        });
                        return Optional.of(Resource.networkBuilder()
                                .id(resultSet.getString(1)) // id is first
                                .variantNum(variantNum)
                                .attributes(attributes)
                                .build());
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    @FunctionalInterface
    interface SqlExecutor {

        void execute(Connection connection) throws SQLException;
    }

    private static void restoreAutoCommitQuietly(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            LOGGER.error("Exception during autocommit restoration, please check next exception", e);
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOGGER.error("Exception during rollback, please check next exception", e);
        }
    }

    private static void executeWithoutAutoCommit(Connection connection, SqlExecutor executor) throws SQLException {
        connection.setAutoCommit(false);
        try {
            executor.execute(connection);
            connection.commit();
        } catch (Exception e) {
            rollbackQuietly(connection);
            throw new RuntimeException(e);
        } finally {
            restoreAutoCommitQuietly(connection);
        }
    }

    private void executeWithoutAutoCommit(SqlExecutor executor) {
        try (var connection = dataSource.getConnection()) {
            executeWithoutAutoCommit(connection, executor);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void createNetworks(List<Resource<NetworkAttributes>> resources) {
        executeWithoutAutoCommit(connection -> createNetworks(connection, resources));
    }

    private void createNetworks(Connection connection, List<Resource<NetworkAttributes>> resources) throws SQLException {
        var tableMapping = mappings.getNetworkMappings();
        try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertNetworkQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet()))) {
            List<Object> values = new ArrayList<>(2 + tableMapping.getColumnsMapping().size());
            for (List<Resource<NetworkAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                for (Resource<NetworkAttributes> resource : subResources) {
                    NetworkAttributes attributes = resource.getAttributes();
                    values.clear();
                    values.add(resource.getVariantNum());
                    values.add(resource.getId());
                    for (var mapping : tableMapping.getColumnsMapping().values()) {
                        values.add(mapping.get(attributes));
                    }
                    bindValues(preparedStmt, values, mapper);
                    preparedStmt.addBatch();
                }
                preparedStmt.executeBatch();
            }
        }
    }

    public void updateNetworks(List<Resource<NetworkAttributes>> resources) {
        executeWithoutAutoCommit(connection -> {
            TableMapping networkMapping = mappings.getNetworkMappings();
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateNetworkQuery(networkMapping.getColumnsMapping().keySet()))) {
                List<Object> values = new ArrayList<>(3 + networkMapping.getColumnsMapping().size());
                for (List<Resource<NetworkAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<NetworkAttributes> resource : subResources) {
                        NetworkAttributes attributes = resource.getAttributes();
                        values.clear();
                        values.add(resource.getId());
                        for (var e : networkMapping.getColumnsMapping().entrySet()) {
                            String columnName = e.getKey();
                            var mapping = e.getValue();
                            if (!columnName.equals(UUID_COLUMN) && !columnName.equals(VARIANT_ID_COLUMN)) {
                                values.add(mapping.get(attributes));
                            }
                        }
                        values.add(attributes.getUuid());
                        values.add(resource.getVariantNum());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        });
    }

    public void deleteNetwork(UUID uuid) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteNetworkQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.execute();
            }
            for (String table : ELEMENT_TABLES) {
                try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteIdentifiablesQuery(table))) {
                    preparedStmt.setObject(1, uuid);
                    preparedStmt.execute();
                }
            }
            // Delete of the temporary limits (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTemporaryLimitsQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.executeUpdate();
            }

            // Delete permanent limits (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeletePermanentLimitsQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.executeUpdate();
            }

            // Delete of the reactive capability curve points (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteReactiveCapabilityCurvePointsQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.executeUpdate();
            }

            // Delete of the tap changer steps (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTapChangerStepQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.executeUpdate();
            }

            // Delete of the extensions (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteExtensionsQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    /**
     * Just delete one variant of the network
     */
    public void deleteNetwork(UUID uuid, int variantNum) {
        if (variantNum == Resource.INITIAL_VARIANT_NUM) {
            throw new IllegalArgumentException("Cannot delete initial variant");
        }
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteNetworkVariantQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.setInt(2, variantNum);
                preparedStmt.execute();
            }
            for (String table : ELEMENT_TABLES) {
                try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteIdentifiablesVariantQuery(table))) {
                    preparedStmt.setObject(1, uuid);
                    preparedStmt.setInt(2, variantNum);
                    preparedStmt.execute();
                }
            }
            // Delete of the temporary limits (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTemporaryLimitsVariantQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.setInt(2, variantNum);
                preparedStmt.executeUpdate();
            }

            // Delete permanent limits (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeletePermanentLimitsVariantQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.setInt(2, variantNum);
                preparedStmt.executeUpdate();
            }

            // Delete of the reactive capability curve points (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteReactiveCapabilityCurvePointsVariantQuery())) {
                preparedStmt.setObject(1, uuid.toString());
                preparedStmt.setInt(2, variantNum);
                preparedStmt.executeUpdate();
            }

            // Delete of the Tap Changer steps (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTapChangerStepVariantQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.setInt(2, variantNum);
                preparedStmt.executeUpdate();
            }

            // Delete of the extensions (which are not Identifiables objects)
            try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteExtensionsVariantQuery())) {
                preparedStmt.setObject(1, uuid);
                preparedStmt.setInt(2, variantNum);
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void cloneNetwork(UUID targetNetworkUuid, UUID sourceNetworkUuid, List<String> targetVariantIds) {
        LOGGER.info("Cloning network {} to network {} with variants {}", sourceNetworkUuid, targetNetworkUuid,
                targetVariantIds.stream().map(LogUtils::sanitizeParam).collect(Collectors.toList()));

        var stopwatch = Stopwatch.createStarted();

        List<VariantInfos> variantsInfoList = getVariantsInfos(sourceNetworkUuid).stream()
                .filter(v -> targetVariantIds.contains(v.getId()))
                .sorted(Comparator.comparing(VariantInfos::getNum))
                .collect(Collectors.toList());

        Set<String> variantsNotFound = new HashSet<>(targetVariantIds);
        List<VariantInfos> newNetworkVariants = new ArrayList<>();

        executeWithoutAutoCommit(connection -> {
            for (VariantInfos variantInfos : variantsInfoList) {
                Resource<NetworkAttributes> sourceNetworkAttribute = getNetwork(sourceNetworkUuid, variantInfos.getNum()).orElseThrow(() -> new PowsyblException("Cannot retrieve source network attributes uuid : " + sourceNetworkUuid + ", variantId : " + variantInfos.getId()));
                sourceNetworkAttribute.getAttributes().setUuid(targetNetworkUuid);
                sourceNetworkAttribute.setVariantNum(VariantUtils.findFistAvailableVariantNum(newNetworkVariants));

                newNetworkVariants.add(new VariantInfos(sourceNetworkAttribute.getAttributes().getVariantId(), sourceNetworkAttribute.getVariantNum()));
                variantsNotFound.remove(sourceNetworkAttribute.getAttributes().getVariantId());

                createNetworks(connection, List.of(sourceNetworkAttribute));
                cloneNetworkElements(connection, sourceNetworkUuid, targetNetworkUuid, sourceNetworkAttribute.getVariantNum(), variantInfos.getNum());
            }
        });

        variantsNotFound.forEach(variantNotFound -> LOGGER.warn("The network {} has no variant ID named : {}, thus it has not been cloned", sourceNetworkUuid, variantNotFound));

        stopwatch.stop();
        LOGGER.info("Network clone done in {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public void cloneNetworkVariant(UUID uuid, int sourceVariantNum, int targetVariantNum, String targetVariantId) {
        String nonNullTargetVariantId = targetVariantId == null ? "variant-" + UUID.randomUUID() : targetVariantId;
        LOGGER.info("Cloning network {} variant {} to variant {}", uuid, sourceVariantNum, targetVariantNum);

        var stopwatch = Stopwatch.createStarted();

        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildCloneNetworksQuery(mappings.getNetworkMappings().getColumnsMapping().keySet()))) {
                preparedStmt.setInt(1, targetVariantNum);
                preparedStmt.setString(2, nonNullTargetVariantId);
                preparedStmt.setObject(3, uuid);
                preparedStmt.setInt(4, sourceVariantNum);
                preparedStmt.execute();
            }

            cloneNetworkElements(connection, uuid, uuid, sourceVariantNum, targetVariantNum);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }

        stopwatch.stop();
        LOGGER.info("Network variant clone done in {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public void cloneNetworkElements(Connection connection, UUID uuid, UUID targetUuid, int sourceVariantNum, int targetVariantNum) throws SQLException {
        for (String tableName : ELEMENT_TABLES) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildCloneIdentifiablesQuery(tableName, mappings.getTableMapping(tableName.toLowerCase()).getColumnsMapping().keySet()))) {
                preparedStmt.setInt(1, targetVariantNum);
                preparedStmt.setObject(2, targetUuid);
                preparedStmt.setObject(3, uuid);
                preparedStmt.setInt(4, sourceVariantNum);
                preparedStmt.execute();
            }
        }
        // Copy of the temporary limits (which are not Identifiables objects)
        try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildCloneTemporaryLimitsQuery())) {
            preparedStmt.setString(1, targetUuid.toString());
            preparedStmt.setInt(2, targetVariantNum);
            preparedStmt.setString(3, uuid.toString());
            preparedStmt.setInt(4, sourceVariantNum);
            preparedStmt.execute();
        }

        // Copy of the permanent limits (which are not Identifiables objects)
        try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildClonePermanentLimitsQuery())) {
            preparedStmt.setString(1, targetUuid.toString());
            preparedStmt.setInt(2, targetVariantNum);
            preparedStmt.setString(3, uuid.toString());
            preparedStmt.setInt(4, sourceVariantNum);
            preparedStmt.execute();
        }

        // Copy of the reactive capability curve points (which are not Identifiables objects)
        try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildCloneReactiveCapabilityCurvePointsQuery())) {
            preparedStmt.setString(1, targetUuid.toString());
            preparedStmt.setInt(2, targetVariantNum);
            preparedStmt.setString(3, uuid.toString());
            preparedStmt.setInt(4, sourceVariantNum);
            preparedStmt.execute();
        }

        // Copy of the Tap Changer steps (which are not Identifiables objects)
        try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildCloneTapChangerStepQuery())) {
            preparedStmt.setObject(1, targetUuid);
            preparedStmt.setInt(2, targetVariantNum);
            preparedStmt.setObject(3, uuid);
            preparedStmt.setInt(4, sourceVariantNum);
            preparedStmt.execute();
        }

        // Copy of the Extensions (which are not Identifiables objects)
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildCloneExtensionsQuery())) {
            preparedStmt.setObject(1, targetUuid);
            preparedStmt.setInt(2, targetVariantNum);
            preparedStmt.setObject(3, uuid);
            preparedStmt.setInt(4, sourceVariantNum);
            preparedStmt.execute();
        }
    }

    public void cloneNetwork(UUID networkUuid, String sourceVariantId, String targetVariantId, boolean mayOverwrite) {
        List<VariantInfos> variantsInfos = getVariantsInfos(networkUuid);
        Optional<VariantInfos> targetVariant = VariantUtils.getVariant(targetVariantId, variantsInfos);
        if (targetVariant.isPresent()) {
            if (!mayOverwrite) {
                throw new JsonApiErrorResponseException(ErrorObject.cloneOverExisting(targetVariantId));
            } else {
                if (Resource.INITIAL_VARIANT_NUM == targetVariant.get().getNum()) {
                    throw new JsonApiErrorResponseException(ErrorObject.cloneOverInitialForbidden());
                }
                deleteNetwork(networkUuid, targetVariant.get().getNum());
            }
        }
        int sourceVariantNum = VariantUtils.getVariantNum(sourceVariantId, variantsInfos);
        int targetVariantNum = VariantUtils.findFistAvailableVariantNum(variantsInfos);
        cloneNetworkVariant(networkUuid, sourceVariantNum, targetVariantNum, targetVariantId);
    }

    public <T extends IdentifiableAttributes> void createIdentifiables(UUID networkUuid, List<Resource<T>> resources,
                                                                       TableMapping tableMapping) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertIdentifiableQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet()))) {
                List<Object> values = new ArrayList<>(3 + tableMapping.getColumnsMapping().size());
                for (List<Resource<T>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<T> resource : subResources) {
                        T attributes = resource.getAttributes();
                        values.clear();
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        for (var mapping : tableMapping.getColumnsMapping().values()) {
                            values.add(mapping.get(attributes));
                        }
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        extensionHandler.insertExtensions(extensionHandler.getExtensionsFromEquipments(networkUuid, resources));
    }

    private <T extends IdentifiableAttributes> Optional<Resource<T>> getIdentifiable(UUID networkUuid, int variantNum, String equipmentId,
                                                                                     TableMapping tableMapping) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetIdentifiableQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet()));
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, equipmentId);
            try (ResultSet resultSet = preparedStmt.executeQuery()) {
                if (resultSet.next()) {
                    T attributes = (T) tableMapping.getAttributesSupplier().get();
                    MutableInt columnIndex = new MutableInt(1);
                    tableMapping.getColumnsMapping().forEach((columnName, columnMapping) -> {
                        bindAttributes(resultSet, columnIndex.getValue(), columnMapping, attributes, mapper);
                        columnIndex.increment();
                    });
                    Resource.Builder<T> resourceBuilder = (Resource.Builder<T>) tableMapping.getResourceBuilderSupplier().get();
                    Resource<T> resource = resourceBuilder
                            .id(equipmentId)
                            .variantNum(variantNum)
                            .attributes(attributes)
                            .build();
                    return Optional.of(completeResourceInfos(resource, networkUuid, variantNum, equipmentId));
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private <T extends IdentifiableAttributes> Resource<T> completeResourceInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensionAttributes = extensionHandler.getExtensions(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        extensionHandler.insertExtensionsInEquipments(networkUuid, List.of((Resource<BatteryAttributes>) resource), extensionAttributes);
        switch (resource.getType()) {
            case GENERATOR:
                return completeGeneratorInfos(resource, networkUuid, variantNum, equipmentId);
            case BATTERY:
                return completeBatteryInfos(resource, networkUuid, variantNum, equipmentId);
            case LINE:
                return completeLineInfos(resource, networkUuid, variantNum, equipmentId);
            case TWO_WINDINGS_TRANSFORMER:
                return completeTwoWindingsTransformerInfos(resource, networkUuid, variantNum, equipmentId);
            case THREE_WINDINGS_TRANSFORMER:
                return completeThreeWindingsTransformerInfos(resource, networkUuid, variantNum, equipmentId);
            case VSC_CONVERTER_STATION:
                return completeVscConverterStationInfos(resource, networkUuid, variantNum, equipmentId);
            case DANGLING_LINE:
                return completeDanglingLineInfos(resource, networkUuid, variantNum, equipmentId);
            default: return resource;
        }
    }

    private <T extends IdentifiableAttributes> Resource<T> completeGeneratorInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, List.of((Resource<GeneratorAttributes>) resource), reactiveCapabilityCurvePoints);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeBatteryInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, List.of((Resource<BatteryAttributes>) resource), reactiveCapabilityCurvePoints);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeLineInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertLimitsInEquipments(networkUuid, List.of((Resource<LineAttributes>) resource), limitsInfos);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeTwoWindingsTransformerInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertLimitsInEquipments(networkUuid, List.of((Resource<TwoWindingsTransformerAttributes>) resource), limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertTapChangerStepsInEquipments(networkUuid, List.of((Resource<TwoWindingsTransformerAttributes>) resource), tapChangerSteps);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeThreeWindingsTransformerInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertLimitsInEquipments(networkUuid, List.of((Resource<ThreeWindingsTransformerAttributes>) resource), limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertTapChangerStepsInEquipments(networkUuid, List.of((Resource<ThreeWindingsTransformerAttributes>) resource), tapChangerSteps);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeVscConverterStationInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, List.of((Resource<VscConverterStationAttributes>) resource), reactiveCapabilityCurvePoints);
        return resource;
    }

    private <T extends IdentifiableAttributes> Resource<T> completeDanglingLineInfos(Resource<T> resource, UUID networkUuid, int variantNum, String equipmentId) {
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentId);
        insertLimitsInEquipments(networkUuid, List.of((Resource<DanglingLineAttributes>) resource), limitsInfos);
        return resource;
    }

    private <T extends IdentifiableAttributes> List<Resource<T>> getIdentifiablesInternal(int variantNum, PreparedStatement preparedStmt, TableMapping tableMapping) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            List<Resource<T>> resources = new ArrayList<>();
            while (resultSet.next()) {
                // first is ID
                String id = resultSet.getString(1);
                T attributes = (T) tableMapping.getAttributesSupplier().get();
                MutableInt columnIndex = new MutableInt(2);
                tableMapping.getColumnsMapping().forEach((columnName, columnMapping) -> {
                    bindAttributes(resultSet, columnIndex.getValue(), columnMapping, attributes, mapper);
                    columnIndex.increment();
                });
                Resource.Builder<T> resourceBuilder = (Resource.Builder<T>) tableMapping.getResourceBuilderSupplier().get();
                resources.add(resourceBuilder
                        .id(id)
                        .variantNum(variantNum)
                        .attributes(attributes)
                        .build());
            }
            return resources;
        }
    }

    private <T extends IdentifiableAttributes> List<Resource<T>> getIdentifiables(UUID networkUuid, int variantNum,
                                                                                  TableMapping tableMapping) {
        List<Resource<T>> identifiables;
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetIdentifiablesQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet()));
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            identifiables = getIdentifiablesInternal(variantNum, preparedStmt, tableMapping);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensions(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, tableMapping.getResourceType().toString());
        extensionHandler.insertExtensionsInEquipments(networkUuid, identifiables, extensions);
        return identifiables;
    }

    private <T extends IdentifiableAttributes> List<Resource<T>> getIdentifiablesInContainer(UUID networkUuid, int variantNum, String containerId,
                                                                                             Set<String> containerColumns,
                                                                                             TableMapping tableMapping) {
        List<Resource<T>> identifiables;
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetIdentifiablesInContainerQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet(), containerColumns));
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < containerColumns.size(); i++) {
                preparedStmt.setString(3 + i, containerId);
            }
            identifiables = getIdentifiablesInternal(variantNum, preparedStmt, tableMapping);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        List<String> equipmentsIds = identifiables.stream().map(Resource::getId).toList();
        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensionsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);
        extensionHandler.insertExtensionsInEquipments(networkUuid, identifiables, extensions);
        return identifiables;
    }

    private <T extends IdentifiableAttributes> List<Resource<T>> getIdentifiablesInVoltageLevel(UUID networkUuid, int variantNum, String voltageLevelId, TableMapping tableMapping) {
        return getIdentifiablesInContainer(networkUuid, variantNum, voltageLevelId, tableMapping.getVoltageLevelIdColumns(), tableMapping);
    }

    public <T extends IdentifiableAttributes & Contained> void updateIdentifiables(UUID networkUuid, List<Resource<T>> resources,
                                                                                   TableMapping tableMapping, String columnToAddToWhereClause) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateIdentifiableQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet(), columnToAddToWhereClause))) {
                List<Object> values = new ArrayList<>(4 + tableMapping.getColumnsMapping().size());
                for (List<Resource<T>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<T> resource : subResources) {
                        T attributes = resource.getAttributes();
                        values.clear();
                        for (var e : tableMapping.getColumnsMapping().entrySet()) {
                            String columnName = e.getKey();
                            var mapping = e.getValue();
                            if (!columnName.equals(columnToAddToWhereClause)) {
                                values.add(mapping.get(attributes));
                            }
                        }
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        values.add(resource.getAttributes().getContainerIds().iterator().next());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        extensionHandler.updateExtensions(networkUuid, resources);
    }

    public void updateInjectionsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources, String tableName) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateInjectionSvQuery(tableName))) {
                List<Object> values = new ArrayList<>(5);
                for (List<Resource<InjectionSvAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<InjectionSvAttributes> resource : subResources) {
                        InjectionSvAttributes attributes = resource.getAttributes();
                        values.clear();
                        values.add(attributes.getP());
                        values.add(attributes.getQ());
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void updateBranchesSv(UUID networkUuid, List<Resource<BranchSvAttributes>> resources, String tableName) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateBranchSvQuery(tableName))) {
                List<Object> values = new ArrayList<>(7);
                for (List<Resource<BranchSvAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<BranchSvAttributes> resource : subResources) {
                        BranchSvAttributes attributes = resource.getAttributes();
                        values.clear();
                        values.add(attributes.getP1());
                        values.add(attributes.getQ1());
                        values.add(attributes.getP2());
                        values.add(attributes.getQ2());
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public <T extends IdentifiableAttributes> void updateIdentifiables(UUID networkUuid, List<Resource<T>> resources,
                                                                       TableMapping tableMapping) {
        executeWithoutAutoCommit(connection -> {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateIdentifiableQuery(tableMapping.getTable(), tableMapping.getColumnsMapping().keySet(), null))) {
                List<Object> values = new ArrayList<>(3 + tableMapping.getColumnsMapping().size());
                for (List<Resource<T>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<T> resource : subResources) {
                        T attributes = resource.getAttributes();
                        values.clear();
                        for (var mapping : tableMapping.getColumnsMapping().values()) {
                            values.add(mapping.get(attributes));
                        }
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        });
        extensionHandler.updateExtensions(networkUuid, resources);
    }

    public void deleteIdentifiable(UUID networkUuid, int variantNum, String id, String tableName) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteIdentifiableQuery(tableName))) {
                preparedStmt.setObject(1, networkUuid);
                preparedStmt.setInt(2, variantNum);
                preparedStmt.setString(3, id);
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        extensionHandler.deleteExtensions(networkUuid, variantNum, id);
    }

    // substation

    public List<Resource<SubstationAttributes>> getSubstations(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getSubstationMappings());
    }

    public Optional<Resource<SubstationAttributes>> getSubstation(UUID networkUuid, int variantNum, String substationId) {
        return getIdentifiable(networkUuid, variantNum, substationId, mappings.getSubstationMappings());
    }

    public void createSubstations(UUID networkUuid, List<Resource<SubstationAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getSubstationMappings());
    }

    public void updateSubstations(UUID networkUuid, List<Resource<SubstationAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getSubstationMappings());
    }

    public void deleteSubstation(UUID networkUuid, int variantNum, String substationId) {
        deleteIdentifiable(networkUuid, variantNum, substationId, SUBSTATION_TABLE);
    }

    // voltage level

    public void createVoltageLevels(UUID networkUuid, List<Resource<VoltageLevelAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getVoltageLevelMappings());
    }

    public void updateVoltageLevels(UUID networkUuid, List<Resource<VoltageLevelAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getVoltageLevelMappings(), SUBSTATION_ID);
    }

    public void updateVoltageLevelsSv(UUID networkUuid, List<Resource<VoltageLevelSvAttributes>> resources) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateVoltageLevelSvQuery())) {
                List<Object> values = new ArrayList<>(5);
                for (List<Resource<VoltageLevelSvAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<VoltageLevelSvAttributes> resource : subResources) {
                        VoltageLevelSvAttributes attributes = resource.getAttributes();
                        values.clear();
                        values.add(attributes.getCalculatedBusesForBusView());
                        values.add(attributes.getCalculatedBusesForBusBreakerView());
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public List<Resource<VoltageLevelAttributes>> getVoltageLevels(UUID networkUuid, int variantNum, String substationId) {
        return getIdentifiablesInContainer(networkUuid, variantNum, substationId, Set.of(SUBSTATION_ID), mappings.getVoltageLevelMappings());
    }

    public Optional<Resource<VoltageLevelAttributes>> getVoltageLevel(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiable(networkUuid, variantNum, voltageLevelId, mappings.getVoltageLevelMappings());
    }

    public List<Resource<VoltageLevelAttributes>> getVoltageLevels(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getVoltageLevelMappings());
    }

    public void deleteVoltageLevel(UUID networkUuid, int variantNum, String voltageLevelId) {
        deleteIdentifiable(networkUuid, variantNum, voltageLevelId, VOLTAGE_LEVEL_TABLE);
    }

    // generator

    public void createGenerators(UUID networkUuid, List<Resource<GeneratorAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getGeneratorMappings());

        // Now that generators are created, we will insert in the database the corresponding reactive capability curve points.
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public Optional<Resource<GeneratorAttributes>> getGenerator(UUID networkUuid, int variantNum, String generatorId) {
        return getIdentifiable(networkUuid, variantNum, generatorId, mappings.getGeneratorMappings());
    }

    public List<Resource<GeneratorAttributes>> getGenerators(UUID networkUuid, int variantNum) {
        List<Resource<GeneratorAttributes>> generators = getIdentifiables(networkUuid, variantNum, mappings.getGeneratorMappings());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.GENERATOR.toString());

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, generators, reactiveCapabilityCurvePoints);

        return generators;
    }

    public List<Resource<GeneratorAttributes>> getVoltageLevelGenerators(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<GeneratorAttributes>> generators = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getGeneratorMappings());

        List<String> equipmentsIds = generators.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePointsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, generators, reactiveCapabilityCurvePoints);

        return generators;
    }

    public void updateGenerators(UUID networkUuid, List<Resource<GeneratorAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getGeneratorMappings(), VOLTAGE_LEVEL_ID_COLUMN);

        // To update the generator's reactive capability curve points, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the reactive capability curve point's primary key is to be
        // modified because of the updated equipment's new values.
        deleteReactiveCapabilityCurvePoints(networkUuid, resources);
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public void updateGeneratorsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, GENERATOR_TABLE);
    }

    public void deleteGenerator(UUID networkUuid, int variantNum, String generatorId) {
        deleteIdentifiable(networkUuid, variantNum, generatorId, GENERATOR_TABLE);
        deleteReactiveCapabilityCurvePoints(networkUuid, variantNum, generatorId);
    }

    // battery

    public void createBatteries(UUID networkUuid, List<Resource<BatteryAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getBatteryMappings());

        // Now that batteries are created, we will insert in the database the corresponding reactive capability curve points.
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public Optional<Resource<BatteryAttributes>> getBattery(UUID networkUuid, int variantNum, String batteryId) {
        return getIdentifiable(networkUuid, variantNum, batteryId, mappings.getBatteryMappings());
    }

    public List<Resource<BatteryAttributes>> getBatteries(UUID networkUuid, int variantNum) {
        List<Resource<BatteryAttributes>> batteries = getIdentifiables(networkUuid, variantNum, mappings.getBatteryMappings());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.BATTERY.toString());

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, batteries, reactiveCapabilityCurvePoints);

        return batteries;
    }

    public List<Resource<BatteryAttributes>> getVoltageLevelBatteries(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<BatteryAttributes>> batteries = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getBatteryMappings());

        List<String> equipmentsIds = batteries.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePointsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, batteries, reactiveCapabilityCurvePoints);

        return batteries;
    }

    public void updateBatteries(UUID networkUuid, List<Resource<BatteryAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getBatteryMappings(), VOLTAGE_LEVEL_ID_COLUMN);

        // To update the battery's reactive capability curve points, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the reactive capability curve point's primary key is to be
        // modified because of the updated equipment's new values.
        deleteReactiveCapabilityCurvePoints(networkUuid, resources);
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public void updateBatteriesSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, BATTERY_TABLE);
    }

    public void deleteBattery(UUID networkUuid, int variantNum, String batteryId) {
        deleteIdentifiable(networkUuid, variantNum, batteryId, BATTERY_TABLE);
        deleteReactiveCapabilityCurvePoints(networkUuid, variantNum, batteryId);
    }

    // load

    public void createLoads(UUID networkUuid, List<Resource<LoadAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getLoadMappings());
    }

    public Optional<Resource<LoadAttributes>> getLoad(UUID networkUuid, int variantNum, String loadId) {
        return getIdentifiable(networkUuid, variantNum, loadId, mappings.getLoadMappings());
    }

    public List<Resource<LoadAttributes>> getLoads(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getLoadMappings());
    }

    public List<Resource<LoadAttributes>> getVoltageLevelLoads(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getLoadMappings());
    }

    public void updateLoads(UUID networkUuid, List<Resource<LoadAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getLoadMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void updateLoadsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, LOAD_TABLE);
    }

    public void deleteLoad(UUID networkUuid, int variantNum, String loadId) {
        deleteIdentifiable(networkUuid, variantNum, loadId, LOAD_TABLE);
    }

    // shunt compensator

    public void createShuntCompensators(UUID networkUuid, List<Resource<ShuntCompensatorAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getShuntCompensatorMappings());
    }

    public Optional<Resource<ShuntCompensatorAttributes>> getShuntCompensator(UUID networkUuid, int variantNum, String shuntCompensatorId) {
        return getIdentifiable(networkUuid, variantNum, shuntCompensatorId, mappings.getShuntCompensatorMappings());
    }

    public List<Resource<ShuntCompensatorAttributes>> getShuntCompensators(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getShuntCompensatorMappings());
    }

    public List<Resource<ShuntCompensatorAttributes>> getVoltageLevelShuntCompensators(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getShuntCompensatorMappings());
    }

    public void updateShuntCompensators(UUID networkUuid, List<Resource<ShuntCompensatorAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getShuntCompensatorMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void updateShuntCompensatorsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, SHUNT_COMPENSATOR_TABLE);
    }

    public void deleteShuntCompensator(UUID networkUuid, int variantNum, String shuntCompensatorId) {
        deleteIdentifiable(networkUuid, variantNum, shuntCompensatorId, SHUNT_COMPENSATOR_TABLE);
    }

    // VSC converter station

    public void createVscConverterStations(UUID networkUuid, List<Resource<VscConverterStationAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getVscConverterStationMappings());

        // Now that vsc converter stations are created, we will insert in the database the corresponding reactive capability curve points.
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public Optional<Resource<VscConverterStationAttributes>> getVscConverterStation(UUID networkUuid, int variantNum, String vscConverterStationId) {
        return getIdentifiable(networkUuid, variantNum, vscConverterStationId, mappings.getVscConverterStationMappings());
    }

    public List<Resource<VscConverterStationAttributes>> getVscConverterStations(UUID networkUuid, int variantNum) {
        List<Resource<VscConverterStationAttributes>> vscConverterStations = getIdentifiables(networkUuid, variantNum, mappings.getVscConverterStationMappings());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.VSC_CONVERTER_STATION.toString());

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, vscConverterStations, reactiveCapabilityCurvePoints);

        return vscConverterStations;
    }

    public List<Resource<VscConverterStationAttributes>> getVoltageLevelVscConverterStations(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<VscConverterStationAttributes>> vscConverterStations = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getVscConverterStationMappings());

        List<String> equipmentsIds = vscConverterStations.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints = getReactiveCapabilityCurvePointsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);

        insertReactiveCapabilityCurvePointsInEquipments(networkUuid, vscConverterStations, reactiveCapabilityCurvePoints);

        return vscConverterStations;
    }

    public void updateVscConverterStations(UUID networkUuid, List<Resource<VscConverterStationAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getVscConverterStationMappings(), VOLTAGE_LEVEL_ID_COLUMN);

        // To update the vscConverterStation's reactive capability curve points, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the reactive capability curve point's primary key is to be
        // modified because of the updated equipment's new values.
        deleteReactiveCapabilityCurvePoints(networkUuid, resources);
        insertReactiveCapabilityCurvePoints(getReactiveCapabilityCurvePointsFromEquipments(networkUuid, resources));
    }

    public void updateVscConverterStationsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, VSC_CONVERTER_STATION_TABLE);
    }

    public void deleteVscConverterStation(UUID networkUuid, int variantNum, String vscConverterStationId) {
        deleteIdentifiable(networkUuid, variantNum, vscConverterStationId, VSC_CONVERTER_STATION_TABLE);
        deleteReactiveCapabilityCurvePoints(networkUuid, variantNum, vscConverterStationId);
    }

    // LCC converter station

    public void createLccConverterStations(UUID networkUuid, List<Resource<LccConverterStationAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getLccConverterStationMappings());
    }

    public Optional<Resource<LccConverterStationAttributes>> getLccConverterStation(UUID networkUuid, int variantNum, String lccConverterStationId) {
        return getIdentifiable(networkUuid, variantNum, lccConverterStationId, mappings.getLccConverterStationMappings());
    }

    public List<Resource<LccConverterStationAttributes>> getLccConverterStations(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getLccConverterStationMappings());
    }

    public List<Resource<LccConverterStationAttributes>> getVoltageLevelLccConverterStations(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getLccConverterStationMappings());
    }

    public void updateLccConverterStations(UUID networkUuid, List<Resource<LccConverterStationAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getLccConverterStationMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void updateLccConverterStationsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, LCC_CONVERTER_STATION_TABLE);
    }

    public void deleteLccConverterStation(UUID networkUuid, int variantNum, String lccConverterStationId) {
        deleteIdentifiable(networkUuid, variantNum, lccConverterStationId, LCC_CONVERTER_STATION_TABLE);
    }

    // static var compensators

    public void createStaticVarCompensators(UUID networkUuid, List<Resource<StaticVarCompensatorAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getStaticVarCompensatorMappings());
    }

    public Optional<Resource<StaticVarCompensatorAttributes>> getStaticVarCompensator(UUID networkUuid, int variantNum, String staticVarCompensatorId) {
        return getIdentifiable(networkUuid, variantNum, staticVarCompensatorId, mappings.getStaticVarCompensatorMappings());
    }

    public List<Resource<StaticVarCompensatorAttributes>> getStaticVarCompensators(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getStaticVarCompensatorMappings());
    }

    public List<Resource<StaticVarCompensatorAttributes>> getVoltageLevelStaticVarCompensators(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getStaticVarCompensatorMappings());
    }

    public void updateStaticVarCompensators(UUID networkUuid, List<Resource<StaticVarCompensatorAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getStaticVarCompensatorMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void updateStaticVarCompensatorsSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, STATIC_VAR_COMPENSATOR_TABLE);
    }

    public void deleteStaticVarCompensator(UUID networkUuid, int variantNum, String staticVarCompensatorId) {
        deleteIdentifiable(networkUuid, variantNum, staticVarCompensatorId, STATIC_VAR_COMPENSATOR_TABLE);
    }

    // busbar section

    public void createBusbarSections(UUID networkUuid, List<Resource<BusbarSectionAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getBusbarSectionMappings());
    }

    public void updateBusbarSections(UUID networkUuid, List<Resource<BusbarSectionAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getBusbarSectionMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public Optional<Resource<BusbarSectionAttributes>> getBusbarSection(UUID networkUuid, int variantNum, String busbarSectionId) {
        return getIdentifiable(networkUuid, variantNum, busbarSectionId, mappings.getBusbarSectionMappings());
    }

    public List<Resource<BusbarSectionAttributes>> getBusbarSections(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getBusbarSectionMappings());
    }

    public List<Resource<BusbarSectionAttributes>> getVoltageLevelBusbarSections(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getBusbarSectionMappings());
    }

    public void deleteBusBarSection(UUID networkUuid, int variantNum, String busBarSectionId) {
        deleteIdentifiable(networkUuid, variantNum, busBarSectionId, BUSBAR_SECTION_TABLE);
    }

    // switch

    public void createSwitches(UUID networkUuid, List<Resource<SwitchAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getSwitchMappings());
    }

    public Optional<Resource<SwitchAttributes>> getSwitch(UUID networkUuid, int variantNum, String switchId) {
        return getIdentifiable(networkUuid, variantNum, switchId, mappings.getSwitchMappings());
    }

    public List<Resource<SwitchAttributes>> getSwitches(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getSwitchMappings());
    }

    public List<Resource<SwitchAttributes>> getVoltageLevelSwitches(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getSwitchMappings());
    }

    public void updateSwitches(UUID networkUuid, List<Resource<SwitchAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getSwitchMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void deleteSwitch(UUID networkUuid, int variantNum, String switchId) {
        deleteIdentifiable(networkUuid, variantNum, switchId, SWITCH_TABLE);
    }

    // 2 windings transformer

    public void createTwoWindingsTransformers(UUID networkUuid, List<Resource<TwoWindingsTransformerAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getTwoWindingsTransformerMappings());

        // Now that twowindingstransformers are created, we will insert in the database the corresponding temporary limits.
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);

        // Now that twowindingstransformers are created, we will insert in the database the corresponding tap Changer steps.
        insertTapChangerSteps(getTapChangerStepsFromEquipment(networkUuid, resources));
    }

    public Optional<Resource<TwoWindingsTransformerAttributes>> getTwoWindingsTransformer(UUID networkUuid, int variantNum, String twoWindingsTransformerId) {
        return getIdentifiable(networkUuid, variantNum, twoWindingsTransformerId, mappings.getTwoWindingsTransformerMappings());
    }

    public List<Resource<TwoWindingsTransformerAttributes>> getTwoWindingsTransformers(UUID networkUuid, int variantNum) {
        List<Resource<TwoWindingsTransformerAttributes>> twoWindingsTransformers = getIdentifiables(networkUuid, variantNum, mappings.getTwoWindingsTransformerMappings());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.TWO_WINDINGS_TRANSFORMER.toString());
        insertLimitsInEquipments(networkUuid, twoWindingsTransformers, limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.TWO_WINDINGS_TRANSFORMER.toString());
        insertTapChangerStepsInEquipments(networkUuid, twoWindingsTransformers, tapChangerSteps);

        return twoWindingsTransformers;
    }

    public List<Resource<TwoWindingsTransformerAttributes>> getVoltageLevelTwoWindingsTransformers(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<TwoWindingsTransformerAttributes>> twoWindingsTransformers = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getTwoWindingsTransformerMappings());

        List<String> equipmentsIds = twoWindingsTransformers.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);
        insertLimitsInEquipments(networkUuid, twoWindingsTransformers, limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerStepsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);
        insertTapChangerStepsInEquipments(networkUuid, twoWindingsTransformers, tapChangerSteps);

        return twoWindingsTransformers;
    }

    public void updateTwoWindingsTransformers(UUID networkUuid, List<Resource<TwoWindingsTransformerAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getTwoWindingsTransformerMappings());

        // To update the twowindingstransformer's temporary limits, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the temporary limit's primary key is to be
        // modified because of the updated equipment's new values.
        deleteTemporaryLimits(networkUuid, resources);
        deletePermanentLimits(networkUuid, resources);
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);

        deleteTapChangerSteps(networkUuid, resources);
        insertTapChangerSteps(getTapChangerStepsFromEquipment(networkUuid, resources));
    }

    public void updateTwoWindingsTransformersSv(UUID networkUuid, List<Resource<BranchSvAttributes>> resources) {
        updateBranchesSv(networkUuid, resources, TWO_WINDINGS_TRANSFORMER_TABLE);
    }

    public void deleteTwoWindingsTransformer(UUID networkUuid, int variantNum, String twoWindingsTransformerId) {
        deleteIdentifiable(networkUuid, variantNum, twoWindingsTransformerId, TWO_WINDINGS_TRANSFORMER_TABLE);
        deleteTemporaryLimits(networkUuid, variantNum, twoWindingsTransformerId);
        deletePermanentLimits(networkUuid, variantNum, twoWindingsTransformerId);
        deleteTapChangerSteps(networkUuid, variantNum, twoWindingsTransformerId);
    }

    // 3 windings transformer

    public void createThreeWindingsTransformers(UUID networkUuid, List<Resource<ThreeWindingsTransformerAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getThreeWindingsTransformerMappings());

        // Now that threewindingstransformers are created, we will insert in the database the corresponding temporary limits.
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);

        // Now that threewindingstransformers are created, we will insert in the database the corresponding tap Changer steps.
        insertTapChangerSteps(getTapChangerStepsFromEquipment(networkUuid, resources));
    }

    public Optional<Resource<ThreeWindingsTransformerAttributes>> getThreeWindingsTransformer(UUID networkUuid, int variantNum, String threeWindingsTransformerId) {
        return getIdentifiable(networkUuid, variantNum, threeWindingsTransformerId, mappings.getThreeWindingsTransformerMappings());
    }

    public List<Resource<ThreeWindingsTransformerAttributes>> getThreeWindingsTransformers(UUID networkUuid, int variantNum) {
        List<Resource<ThreeWindingsTransformerAttributes>> threeWindingsTransformers = getIdentifiables(networkUuid, variantNum, mappings.getThreeWindingsTransformerMappings());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.THREE_WINDINGS_TRANSFORMER.toString());
        insertLimitsInEquipments(networkUuid, threeWindingsTransformers, limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.THREE_WINDINGS_TRANSFORMER.toString());
        insertTapChangerStepsInEquipments(networkUuid, threeWindingsTransformers, tapChangerSteps);

        return threeWindingsTransformers;
    }

    public List<Resource<ThreeWindingsTransformerAttributes>> getVoltageLevelThreeWindingsTransformers(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<ThreeWindingsTransformerAttributes>> threeWindingsTransformers = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getThreeWindingsTransformerMappings());

        List<String> equipmentsIds = threeWindingsTransformers.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);
        insertLimitsInEquipments(networkUuid, threeWindingsTransformers, limitsInfos);

        Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps = getTapChangerStepsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);
        insertTapChangerStepsInEquipments(networkUuid, threeWindingsTransformers, tapChangerSteps);

        return threeWindingsTransformers;
    }

    public void updateThreeWindingsTransformers(UUID networkUuid, List<Resource<ThreeWindingsTransformerAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getThreeWindingsTransformerMappings());

        // To update the threewindingstransformer's temporary limits, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the temporary limit's primary key is to be
        // modified because of the updated equipment's new values.
        deleteTemporaryLimits(networkUuid, resources);
        deletePermanentLimits(networkUuid, resources);
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);

        deleteTapChangerSteps(networkUuid, resources);
        insertTapChangerSteps(getTapChangerStepsFromEquipment(networkUuid, resources));
    }

    public void updateThreeWindingsTransformersSv(UUID networkUuid, List<Resource<ThreeWindingsTransformerSvAttributes>> resources) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildUpdateThreeWindingsTransformerSvQuery())) {
                List<Object> values = new ArrayList<>(9);
                for (List<Resource<ThreeWindingsTransformerSvAttributes>> subResources : Lists.partition(resources, BATCH_SIZE)) {
                    for (Resource<ThreeWindingsTransformerSvAttributes> resource : subResources) {
                        ThreeWindingsTransformerSvAttributes attributes = resource.getAttributes();
                        values.clear();
                        values.add(attributes.getP1());
                        values.add(attributes.getQ1());
                        values.add(attributes.getP2());
                        values.add(attributes.getQ2());
                        values.add(attributes.getP3());
                        values.add(attributes.getQ3());
                        values.add(networkUuid);
                        values.add(resource.getVariantNum());
                        values.add(resource.getId());
                        bindValues(preparedStmt, values, mapper);
                        preparedStmt.addBatch();
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void deleteThreeWindingsTransformer(UUID networkUuid, int variantNum, String threeWindingsTransformerId) {
        deleteIdentifiable(networkUuid, variantNum, threeWindingsTransformerId, THREE_WINDINGS_TRANSFORMER_TABLE);
        deleteTemporaryLimits(networkUuid, variantNum, threeWindingsTransformerId);
        deletePermanentLimits(networkUuid, variantNum, threeWindingsTransformerId);
        deleteTapChangerSteps(networkUuid, variantNum, threeWindingsTransformerId);
    }

    // line

    public void createLines(UUID networkUuid, List<Resource<LineAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getLineMappings());

        // Now that lines are created, we will insert in the database the corresponding temporary limits.
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);
    }

    public Optional<Resource<LineAttributes>> getLine(UUID networkUuid, int variantNum, String lineId) {
        return getIdentifiable(networkUuid, variantNum, lineId, mappings.getLineMappings());
    }

    public List<Resource<LineAttributes>> getLines(UUID networkUuid, int variantNum) {
        List<Resource<LineAttributes>> lines = getIdentifiables(networkUuid, variantNum, mappings.getLineMappings());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.LINE.toString());

        insertLimitsInEquipments(networkUuid, lines, limitsInfos);

        return lines;
    }

    public List<Resource<LineAttributes>> getVoltageLevelLines(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<LineAttributes>> lines = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getLineMappings());

        List<String> equipmentsIds = lines.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);

        insertLimitsInEquipments(networkUuid, lines, limitsInfos);

        return lines;
    }

    public void updateLines(UUID networkUuid, List<Resource<LineAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getLineMappings());

        // To update the line's temporary limits, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the temporary limit's primary key is to be
        // modified because of the updated equipment's new values.
        deleteTemporaryLimits(networkUuid, resources);
        deletePermanentLimits(networkUuid, resources);
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);
    }

    public void updateLinesSv(UUID networkUuid, List<Resource<BranchSvAttributes>> resources) {
        updateBranchesSv(networkUuid, resources, LINE_TABLE);
    }

    public void deleteLine(UUID networkUuid, int variantNum, String lineId) {
        deleteIdentifiable(networkUuid, variantNum, lineId, LINE_TABLE);
        deleteTemporaryLimits(networkUuid, variantNum, lineId);
        deletePermanentLimits(networkUuid, variantNum, lineId);
    }

    // Hvdc line

    public List<Resource<HvdcLineAttributes>> getHvdcLines(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getHvdcLineMappings());
    }

    public Optional<Resource<HvdcLineAttributes>> getHvdcLine(UUID networkUuid, int variantNum, String hvdcLineId) {
        return getIdentifiable(networkUuid, variantNum, hvdcLineId, mappings.getHvdcLineMappings());
    }

    public void createHvdcLines(UUID networkUuid, List<Resource<HvdcLineAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getHvdcLineMappings());
    }

    public void updateHvdcLines(UUID networkUuid, List<Resource<HvdcLineAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getHvdcLineMappings());
    }

    public void deleteHvdcLine(UUID networkUuid, int variantNum, String hvdcLineId) {
        deleteIdentifiable(networkUuid, variantNum, hvdcLineId, HVDC_LINE_TABLE);
    }

    // Dangling line

    public List<Resource<DanglingLineAttributes>> getDanglingLines(UUID networkUuid, int variantNum) {
        List<Resource<DanglingLineAttributes>> danglingLines = getIdentifiables(networkUuid, variantNum, mappings.getDanglingLineMappings());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfos(networkUuid, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.DANGLING_LINE.toString());

        insertLimitsInEquipments(networkUuid, danglingLines, limitsInfos);

        return danglingLines;
    }

    public Optional<Resource<DanglingLineAttributes>> getDanglingLine(UUID networkUuid, int variantNum, String danglingLineId) {
        return getIdentifiable(networkUuid, variantNum, danglingLineId, mappings.getDanglingLineMappings());
    }

    public List<Resource<DanglingLineAttributes>> getVoltageLevelDanglingLines(UUID networkUuid, int variantNum, String voltageLevelId) {
        List<Resource<DanglingLineAttributes>> danglingLines = getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getDanglingLineMappings());

        List<String> equipmentsIds = danglingLines.stream().map(Resource::getId).collect(Collectors.toList());

        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, equipmentsIds);

        insertLimitsInEquipments(networkUuid, danglingLines, limitsInfos);

        return danglingLines;
    }

    public void createDanglingLines(UUID networkUuid, List<Resource<DanglingLineAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getDanglingLineMappings());

        // Now that the dangling lines are created, we will insert in the database the corresponding temporary limits.
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);
    }

    public void deleteDanglingLine(UUID networkUuid, int variantNum, String danglingLineId) {
        deleteIdentifiable(networkUuid, variantNum, danglingLineId, DANGLING_LINE_TABLE);
        deleteTemporaryLimits(networkUuid, variantNum, danglingLineId);
        deletePermanentLimits(networkUuid, variantNum, danglingLineId);
    }

    public void updateDanglingLines(UUID networkUuid, List<Resource<DanglingLineAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getDanglingLineMappings(), VOLTAGE_LEVEL_ID_COLUMN);

        // To update the danglingline's temporary limits, we will first delete them, then create them again.
        // This is done this way to prevent issues in case the temporary limit's primary key is to be
        // modified because of the updated equipment's new values.
        deleteTemporaryLimits(networkUuid, resources);
        deletePermanentLimits(networkUuid, resources);
        Map<OwnerInfo, LimitsInfos> limitsInfos = getLimitsInfosFromEquipments(networkUuid, resources);
        insertTemporaryLimits(limitsInfos);
        insertPermanentLimits(limitsInfos);
    }

    public void updateDanglingLinesSv(UUID networkUuid, List<Resource<InjectionSvAttributes>> resources) {
        updateInjectionsSv(networkUuid, resources, DANGLING_LINE_TABLE);
    }

    // Tie lines

    public List<Resource<TieLineAttributes>> getTieLines(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getTieLineMappings());
    }

    public Optional<Resource<TieLineAttributes>> getTieLine(UUID networkUuid, int variantNum, String tieLineId) {
        return getIdentifiable(networkUuid, variantNum, tieLineId, mappings.getTieLineMappings());
    }

    public void createTieLines(UUID networkUuid, List<Resource<TieLineAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getTieLineMappings());
    }

    public void deleteTieLine(UUID networkUuid, int variantNum, String tieLineId) {
        deleteIdentifiable(networkUuid, variantNum, tieLineId, TIE_LINE_TABLE);
        deleteTemporaryLimits(networkUuid, variantNum, tieLineId);
        deletePermanentLimits(networkUuid, variantNum, tieLineId);
    }

    public void updateTieLines(UUID networkUuid, List<Resource<TieLineAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getTieLineMappings());
    }

    // configured buses

    public void createBuses(UUID networkUuid, List<Resource<ConfiguredBusAttributes>> resources) {
        createIdentifiables(networkUuid, resources, mappings.getConfiguredBusMappings());
    }

    public Optional<Resource<ConfiguredBusAttributes>> getConfiguredBus(UUID networkUuid, int variantNum, String busId) {
        return getIdentifiable(networkUuid, variantNum, busId, mappings.getConfiguredBusMappings());
    }

    public List<Resource<ConfiguredBusAttributes>> getConfiguredBuses(UUID networkUuid, int variantNum) {
        return getIdentifiables(networkUuid, variantNum, mappings.getConfiguredBusMappings());
    }

    public List<Resource<ConfiguredBusAttributes>> getVoltageLevelBuses(UUID networkUuid, int variantNum, String voltageLevelId) {
        return getIdentifiablesInVoltageLevel(networkUuid, variantNum, voltageLevelId, mappings.getConfiguredBusMappings());
    }

    public void updateBuses(UUID networkUuid, List<Resource<ConfiguredBusAttributes>> resources) {
        updateIdentifiables(networkUuid, resources, mappings.getConfiguredBusMappings(), VOLTAGE_LEVEL_ID_COLUMN);
    }

    public void deleteBus(UUID networkUuid, int variantNum, String configuredBusId) {
        deleteIdentifiable(networkUuid, variantNum, configuredBusId, CONFIGURED_BUS_TABLE);
    }

    private static String getNonEmptyTable(ResultSet resultSet) throws SQLException {
        var metaData = resultSet.getMetaData();
        for (int col = 4; col <= metaData.getColumnCount(); col++) { // skip 3 first columns corresponding to first inner select
            if (metaData.getColumnName(col).equalsIgnoreCase(ID_COLUMN) && resultSet.getObject(col) != null) {
                return metaData.getTableName(col).toLowerCase();
            }
        }
        return null;
    }

    private static Map<Pair<String, String>, Integer> getColumnIndexByTableNameAndColumnName(ResultSet resultSet, String tableName) throws SQLException {
        Map<Pair<String, String>, Integer> columnIndexes = new HashMap<>();
        var metaData = resultSet.getMetaData();
        for (int col = 1; col <= metaData.getColumnCount(); col++) {
            if (metaData.getTableName(col).equalsIgnoreCase(tableName)) {
                columnIndexes.put(Pair.of(tableName, metaData.getColumnName(col).toLowerCase()), col);
            }
        }
        return columnIndexes;
    }

    public Optional<Resource<IdentifiableAttributes>> getIdentifiable(UUID networkUuid, int variantNum, String id) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildGetIdentifiableForAllTablesQuery());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, id);
            try (ResultSet resultSet = preparedStmt.executeQuery()) {
                if (resultSet.next()) {
                    String tableName = getNonEmptyTable(resultSet);
                    if (tableName != null) {
                        TableMapping tableMapping = mappings.getTableMapping(tableName);
                        var columnIndexByTableAndColumnName = getColumnIndexByTableNameAndColumnName(resultSet, tableName);

                        IdentifiableAttributes attributes = tableMapping.getAttributesSupplier().get();
                        tableMapping.getColumnsMapping().forEach((columnName, columnMapping) -> {
                            Integer columnIndex = columnIndexByTableAndColumnName.get(Pair.of(tableName, columnName.toLowerCase()));
                            if (columnIndex == null) {
                                throw new PowsyblException("Column '" + columnName.toLowerCase() + "' of table '" + tableName + "' not found");
                            }
                            bindAttributes(resultSet, columnIndex, columnMapping, attributes, mapper);
                        });

                        Resource<IdentifiableAttributes> resource = new Resource.Builder<>(tableMapping.getResourceType())
                                .id(id)
                                .variantNum(variantNum)
                                .attributes(attributes)
                                .build();
                        return Optional.of(completeResourceInfos(resource, networkUuid, variantNum, id));
                    }
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
        return Optional.empty();
    }

    // Temporary Limits
    public Map<OwnerInfo, List<TemporaryLimitAttributes>> getTemporaryLimitsWithInClause(UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildTemporaryLimitWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }

            return innerGetTemporaryLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, List<PermanentLimitAttributes>> getPermanentLimitsWithInClause(UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildPermanentLimitWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }

            return innerGetPermanentLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, LimitsInfos> getLimitsInfos(UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        Map<OwnerInfo, List<TemporaryLimitAttributes>> temporaryLimits = getTemporaryLimits(networkUuid, variantNum, columnNameForWhereClause, valueForWhereClause);
        Map<OwnerInfo, List<PermanentLimitAttributes>> permanentLimits = getPermanentLimits(networkUuid, variantNum, columnNameForWhereClause, valueForWhereClause);
        return mergeLimitsIntoLimitsInfos(temporaryLimits, permanentLimits);
    }

    public Map<OwnerInfo, LimitsInfos> getLimitsInfosWithInClause(UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        Map<OwnerInfo, List<TemporaryLimitAttributes>> temporaryLimits = getTemporaryLimitsWithInClause(networkUuid, variantNum, columnNameForWhereClause, valuesForInClause);
        Map<OwnerInfo, List<PermanentLimitAttributes>> permanentLimits = getPermanentLimitsWithInClause(networkUuid, variantNum, columnNameForWhereClause, valuesForInClause);
        return mergeLimitsIntoLimitsInfos(temporaryLimits, permanentLimits);
    }

    private Map<OwnerInfo, LimitsInfos> mergeLimitsIntoLimitsInfos(Map<OwnerInfo, List<TemporaryLimitAttributes>> temporaryLimits, Map<OwnerInfo, List<PermanentLimitAttributes>> permanentLimits) {
        Map<OwnerInfo, LimitsInfos> limitsInfos = new HashMap<>();
        temporaryLimits.forEach((ownerInfo, temporaryLimitAttributes) -> limitsInfos.put(ownerInfo,
                new LimitsInfos(new ArrayList<>(), temporaryLimitAttributes)));
        permanentLimits.forEach((ownerInfo, permanentLimitAttributes) -> {
            if (limitsInfos.containsKey(ownerInfo)) {
                limitsInfos.get(ownerInfo).getPermanentLimits().addAll(permanentLimitAttributes);
            } else {
                limitsInfos.put(ownerInfo, new LimitsInfos(permanentLimitAttributes, new ArrayList<>()));
            }
        });
        return limitsInfos;
    }

    public Map<OwnerInfo, List<TemporaryLimitAttributes>> getTemporaryLimits(UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildTemporaryLimitQuery(columnNameForWhereClause));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetTemporaryLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, List<PermanentLimitAttributes>> getPermanentLimits(UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildPermanentLimitQuery(columnNameForWhereClause));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetPermanentLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<OwnerInfo, List<TemporaryLimitAttributes>> innerGetTemporaryLimits(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<OwnerInfo, List<TemporaryLimitAttributes>> map = new HashMap<>();
            while (resultSet.next()) {

                OwnerInfo owner = new OwnerInfo();
                TemporaryLimitAttributes temporaryLimit = new TemporaryLimitAttributes();
                // In order, from the QueryCatalog.buildTemporaryLimitQuery SQL query :
                // equipmentId, equipmentType, networkUuid, variantNum, side, limitType, name, value, acceptableDuration, fictitious
                owner.setEquipmentId(resultSet.getString(1));
                owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
                owner.setNetworkUuid(UUID.fromString(resultSet.getString(3)));
                owner.setVariantNum(resultSet.getInt(4));
                temporaryLimit.setOperationalLimitsGroupId(resultSet.getString(5));
                temporaryLimit.setSide(resultSet.getInt(6));
                temporaryLimit.setLimitType(LimitType.valueOf(resultSet.getString(7)));
                temporaryLimit.setName(resultSet.getString(8));
                temporaryLimit.setValue(resultSet.getDouble(9));
                temporaryLimit.setAcceptableDuration(resultSet.getInt(10));
                temporaryLimit.setFictitious(resultSet.getBoolean(11));

                map.computeIfAbsent(owner, k -> new ArrayList<>());
                map.get(owner).add(temporaryLimit);
            }
            return map;
        }
    }

    private Map<OwnerInfo, List<PermanentLimitAttributes>> innerGetPermanentLimits(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<OwnerInfo, List<PermanentLimitAttributes>> map = new HashMap<>();
            while (resultSet.next()) {

                OwnerInfo owner = new OwnerInfo();
                PermanentLimitAttributes permanentLimit = new PermanentLimitAttributes();
                // In order, from the QueryCatalog.buildTemporaryLimitQuery SQL query :
                // equipmentId, equipmentType, networkUuid, variantNum, side, limitType, name, value, acceptableDuration, fictitious
                owner.setEquipmentId(resultSet.getString(1));
                owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
                owner.setNetworkUuid(UUID.fromString(resultSet.getString(3)));
                owner.setVariantNum(resultSet.getInt(4));
                permanentLimit.setOperationalLimitsGroupId(resultSet.getString(5));
                permanentLimit.setSide(resultSet.getInt(6));
                permanentLimit.setLimitType(LimitType.valueOf(resultSet.getString(7)));
                permanentLimit.setValue(resultSet.getDouble(8));

                map.computeIfAbsent(owner, k -> new ArrayList<>());
                map.get(owner).add(permanentLimit);
            }
            return map;
        }
    }

    protected <T extends LimitHolder & IdentifiableAttributes> Map<OwnerInfo, LimitsInfos> getLimitsInfosFromEquipments(UUID networkUuid, List<Resource<T>> resources) {
        Map<OwnerInfo, LimitsInfos> map = new HashMap<>();

        if (!resources.isEmpty()) {
            for (Resource<T> resource : resources) {
                OwnerInfo info = new OwnerInfo(
                    resource.getId(),
                    resource.getType(),
                    networkUuid,
                    resource.getVariantNum()
                );
                T equipment = resource.getAttributes();
                map.put(info, getAllLimitsInfos(equipment));
            }
        }
        return map;
    }

    public void insertTemporaryLimits(Map<OwnerInfo, LimitsInfos> limitsInfos) {
        Map<OwnerInfo, List<TemporaryLimitAttributes>> temporaryLimits = limitsInfos.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getTemporaryLimits()));
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertTemporaryLimitsQuery())) {
                List<Object> values = new ArrayList<>(11);
                List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> list = new ArrayList<>(temporaryLimits.entrySet());
                for (List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> subUnit : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>> entry : subUnit) {
                        for (TemporaryLimitAttributes temporaryLimit : entry.getValue()) {
                            values.clear();
                            // In order, from the QueryCatalog.buildInsertTemporaryLimitsQuery SQL query :
                            // equipmentId, equipmentType, networkUuid, variantNum, operationalLimitsGroupId, side, limitType, name, value, acceptableDuration, fictitious
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(temporaryLimit.getOperationalLimitsGroupId());
                            values.add(temporaryLimit.getSide());
                            values.add(temporaryLimit.getLimitType().toString());
                            values.add(temporaryLimit.getName());
                            values.add(temporaryLimit.getValue());
                            values.add(temporaryLimit.getAcceptableDuration());
                            values.add(temporaryLimit.isFictitious());
                            bindValues(preparedStmt, values, mapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void insertPermanentLimits(Map<OwnerInfo, LimitsInfos> limitsInfos) {
        Map<OwnerInfo, List<PermanentLimitAttributes>> permanentLimits = limitsInfos.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPermanentLimits()));
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertPermanentLimitsQuery())) {
                List<Object> values = new ArrayList<>(8);
                List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> list = new ArrayList<>(permanentLimits.entrySet());
                for (List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> subUnit : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<PermanentLimitAttributes>> entry : subUnit) {
                        for (PermanentLimitAttributes permanentLimit : entry.getValue()) {
                            values.clear();
                            // In order, from the QueryCatalog.buildInsertTemporaryLimitsQuery SQL query :
                            // equipmentId, equipmentType, networkUuid, variantNum, operationalLimitsGroupId, side, limitType, value
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(permanentLimit.getOperationalLimitsGroupId());
                            values.add(permanentLimit.getSide());
                            values.add(permanentLimit.getLimitType().toString());
                            values.add(permanentLimit.getValue());
                            bindValues(preparedStmt, values, mapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    protected <T extends LimitHolder & IdentifiableAttributes> void insertLimitsInEquipments(UUID networkUuid, List<Resource<T>> equipments, Map<OwnerInfo, LimitsInfos> limitsInfos) {

        if (!limitsInfos.isEmpty() && !equipments.isEmpty()) {
            for (Resource<T> equipmentAttributesResource : equipments) {
                OwnerInfo owner = new OwnerInfo(
                    equipmentAttributesResource.getId(),
                    equipmentAttributesResource.getType(),
                    networkUuid,
                    equipmentAttributesResource.getVariantNum()
                );
                if (limitsInfos.containsKey(owner)) {
                    T equipment = equipmentAttributesResource.getAttributes();
                    for (TemporaryLimitAttributes temporaryLimit : limitsInfos.get(owner).getTemporaryLimits()) {
                        insertTemporaryLimitInEquipment(equipment, temporaryLimit);
                    }
                    for (PermanentLimitAttributes permanentLimit : limitsInfos.get(owner).getPermanentLimits()) {
                        insertPermanentLimitInEquipment(equipment, permanentLimit);
                    }
                }
            }
        }
    }

    private <T extends LimitHolder> void insertTemporaryLimitInEquipment(T equipment, TemporaryLimitAttributes temporaryLimit) {
        LimitType type = temporaryLimit.getLimitType();
        int side = temporaryLimit.getSide();
        String groupId = temporaryLimit.getOperationalLimitsGroupId();
        if (getLimits(equipment, type, side, groupId) == null) {
            setLimits(equipment, type, side, new LimitsAttributes(), groupId);
        }
        if (getLimits(equipment, type, side, groupId).getTemporaryLimits() == null) {
            getLimits(equipment, type, side, groupId).setTemporaryLimits(new TreeMap<>());
        }
        getLimits(equipment, type, side, groupId).getTemporaryLimits().put(temporaryLimit.getAcceptableDuration(), temporaryLimit);
    }

    private <T extends LimitHolder> void insertPermanentLimitInEquipment(T equipment, PermanentLimitAttributes permanentLimit) {
        LimitType type = permanentLimit.getLimitType();
        int side = permanentLimit.getSide();
        String groupId = permanentLimit.getOperationalLimitsGroupId();
        if (getLimits(equipment, type, side, groupId) == null) {
            setLimits(equipment, type, side, new LimitsAttributes(), groupId);
        }
        getLimits(equipment, type, side, groupId).setPermanentLimit(permanentLimit.getValue());
    }

    private void deleteTemporaryLimits(UUID networkUuid, int variantNum, String equipmentId) {
        deleteTemporaryLimits(networkUuid, variantNum, List.of(equipmentId));
    }

    private void deletePermanentLimits(UUID networkUuid, int variantNum, String equipmentId) {
        deletePermanentLimits(networkUuid, variantNum, List.of(equipmentId));
    }

    private void deleteTemporaryLimits(UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTemporaryLimitsVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setObject(1, networkUuid.toString());
                preparedStmt.setInt(2, variantNum);
                for (int i = 0; i < equipmentIds.size(); i++) {
                    preparedStmt.setString(3 + i, equipmentIds.get(i));
                }
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private void deletePermanentLimits(UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeletePermanentLimitsVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setObject(1, networkUuid.toString());
                preparedStmt.setInt(2, variantNum);
                for (int i = 0; i < equipmentIds.size(); i++) {
                    preparedStmt.setString(3 + i, equipmentIds.get(i));
                }
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private <T extends IdentifiableAttributes> void deleteTemporaryLimits(UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, List<String>> resourceIdsByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            List<String> resourceIds = resourceIdsByVariant.get(resource.getVariantNum());
            if (resourceIds != null) {
                resourceIds.add(resource.getId());
            } else {
                resourceIds = new ArrayList<>();
                resourceIds.add(resource.getId());
            }
            resourceIdsByVariant.put(resource.getVariantNum(), resourceIds);
        }
        resourceIdsByVariant.forEach((k, v) -> deleteTemporaryLimits(networkUuid, k, v));
    }

    private <T extends IdentifiableAttributes> void deletePermanentLimits(UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, List<String>> resourceIdsByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            List<String> resourceIds = resourceIdsByVariant.get(resource.getVariantNum());
            if (resourceIds != null) {
                resourceIds.add(resource.getId());
            } else {
                resourceIds = new ArrayList<>();
                resourceIds.add(resource.getId());
            }
            resourceIdsByVariant.put(resource.getVariantNum(), resourceIds);
        }
        resourceIdsByVariant.forEach((k, v) -> deletePermanentLimits(networkUuid, k, v));
    }

    // Reactive Capability Curve Points

    public void insertReactiveCapabilityCurvePoints(Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertReactiveCapabilityCurvePointsQuery())) {
                List<Object> values = new ArrayList<>(7);
                List<Map.Entry<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>>> list = new ArrayList<>(reactiveCapabilityCurvePoints.entrySet());
                for (List<Map.Entry<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>>> subUnit : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> myPair : subUnit) {
                        for (ReactiveCapabilityCurvePointAttributes reactiveCapabilityCurvePoint : myPair.getValue()) {
                            values.clear();
                            // In order, from the QueryCatalog.buildInsertReactiveCapabilityCurvePointsQuery SQL query :
                            // equipmentId, equipmentType, networkUuid, variantNum, minQ, maxQ, p
                            values.add(myPair.getKey().getEquipmentId());
                            values.add(myPair.getKey().getEquipmentType().toString());
                            values.add(myPair.getKey().getNetworkUuid());
                            values.add(myPair.getKey().getVariantNum());
                            values.add(reactiveCapabilityCurvePoint.getMinQ());
                            values.add(reactiveCapabilityCurvePoint.getMaxQ());
                            values.add(reactiveCapabilityCurvePoint.getP());
                            bindValues(preparedStmt, values, mapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> getReactiveCapabilityCurvePointsWithInClause(UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildReactiveCapabilityCurvePointWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }

            return innerGetReactiveCapabilityCurvePoints(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> getReactiveCapabilityCurvePoints(UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildReactiveCapabilityCurvePointQuery(columnNameForWhereClause));
            preparedStmt.setObject(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetReactiveCapabilityCurvePoints(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> innerGetReactiveCapabilityCurvePoints(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> map = new HashMap<>();
            while (resultSet.next()) {

                OwnerInfo owner = new OwnerInfo();
                ReactiveCapabilityCurvePointAttributes reactiveCapabilityCurvePoint = new ReactiveCapabilityCurvePointAttributes();
                // In order, from the QueryCatalog.buildReactiveCapabilityCurvePointQuery SQL query :
                // equipmentId, equipmentType, networkUuid, variantNum, minQ, maxQ, p
                owner.setEquipmentId(resultSet.getString(1));
                owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
                owner.setNetworkUuid(UUID.fromString(resultSet.getString(3)));
                owner.setVariantNum(resultSet.getInt(4));
                reactiveCapabilityCurvePoint.setMinQ(resultSet.getDouble(5));
                reactiveCapabilityCurvePoint.setMaxQ(resultSet.getDouble(6));
                reactiveCapabilityCurvePoint.setP(resultSet.getDouble(7));

                map.computeIfAbsent(owner, k -> new ArrayList<>());
                map.get(owner).add(reactiveCapabilityCurvePoint);
            }
            return map;
        }
    }

    protected <T extends ReactiveLimitHolder & IdentifiableAttributes> Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> getReactiveCapabilityCurvePointsFromEquipments(UUID networkUuid, List<Resource<T>> resources) {
        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> map = new HashMap<>();

        if (!resources.isEmpty()) {
            for (Resource<T> resource : resources) {

                ReactiveLimitsAttributes reactiveLimits = resource.getAttributes().getReactiveLimits();
                if (reactiveLimits != null
                        && reactiveLimits.getKind() == ReactiveLimitsKind.CURVE
                        && ((ReactiveCapabilityCurveAttributes) reactiveLimits).getPoints() != null) {

                    OwnerInfo info = new OwnerInfo(
                            resource.getId(),
                            resource.getType(),
                            networkUuid,
                            resource.getVariantNum()
                    );
                    map.put(info, new ArrayList<>(((ReactiveCapabilityCurveAttributes) reactiveLimits).getPoints().values()));
                }
            }
        }
        return map;
    }

    protected <T extends ReactiveLimitHolder & IdentifiableAttributes> void insertReactiveCapabilityCurvePointsInEquipments(UUID networkUuid, List<Resource<T>> equipments, Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> reactiveCapabilityCurvePoints) {

        if (!reactiveCapabilityCurvePoints.isEmpty() && !equipments.isEmpty()) {
            for (Resource<T> equipmentAttributesResource : equipments) {
                OwnerInfo owner = new OwnerInfo(
                        equipmentAttributesResource.getId(),
                        equipmentAttributesResource.getType(),
                        networkUuid,
                        equipmentAttributesResource.getVariantNum()
                );
                if (reactiveCapabilityCurvePoints.containsKey(owner)) {
                    T equipment = equipmentAttributesResource.getAttributes();
                    for (ReactiveCapabilityCurvePointAttributes reactiveCapabilityCurvePoint : reactiveCapabilityCurvePoints.get(owner)) {
                        insertReactiveCapabilityCurvePointInEquipment(equipment, reactiveCapabilityCurvePoint);
                    }
                }
            }
        }
    }

    private <T extends ReactiveLimitHolder> void insertReactiveCapabilityCurvePointInEquipment(T equipment, ReactiveCapabilityCurvePointAttributes reactiveCapabilityCurvePoint) {

        if (equipment.getReactiveLimits() == null) {
            equipment.setReactiveLimits(new ReactiveCapabilityCurveAttributes());
        }
        ReactiveLimitsAttributes reactiveLimitsAttributes = equipment.getReactiveLimits();
        if (reactiveLimitsAttributes instanceof ReactiveCapabilityCurveAttributes) {
            if (((ReactiveCapabilityCurveAttributes) reactiveLimitsAttributes).getPoints() == null) {
                ((ReactiveCapabilityCurveAttributes) reactiveLimitsAttributes).setPoints(new TreeMap<>());
            }
            ((ReactiveCapabilityCurveAttributes) reactiveLimitsAttributes).getPoints().put(reactiveCapabilityCurvePoint.getP(), reactiveCapabilityCurvePoint);
        }
    }

    private void deleteReactiveCapabilityCurvePoints(UUID networkUuid, int variantNum, String equipmentId) {
        deleteReactiveCapabilityCurvePoints(networkUuid, variantNum, List.of(equipmentId));
    }

    private void deleteReactiveCapabilityCurvePoints(UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteReactiveCapabilityCurvePointsVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setObject(1, networkUuid.toString());
                preparedStmt.setInt(2, variantNum);
                for (int i = 0; i < equipmentIds.size(); i++) {
                    preparedStmt.setString(3 + i, equipmentIds.get(i));
                }
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private <T extends IdentifiableAttributes> void deleteReactiveCapabilityCurvePoints(UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, List<String>> resourceIdsByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            List<String> resourceIds = resourceIdsByVariant.get(resource.getVariantNum());
            if (resourceIds != null) {
                resourceIds.add(resource.getId());
            } else {
                resourceIds = new ArrayList<>();
                resourceIds.add(resource.getId());
            }
            resourceIdsByVariant.put(resource.getVariantNum(), resourceIds);
        }
        resourceIdsByVariant.forEach((k, v) -> deleteReactiveCapabilityCurvePoints(networkUuid, k, v));
    }

    // TapChanger Steps

    public Map<OwnerInfo, List<TapChangerStepAttributes>> getTapChangerStepsWithInClause(UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildTapChangerStepWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }
            return innerGetTapChangerSteps(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public Map<OwnerInfo, List<TapChangerStepAttributes>> getTapChangerSteps(UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryCatalog.buildTapChangerStepQuery(columnNameForWhereClause));
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetTapChangerSteps(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<OwnerInfo, List<TapChangerStepAttributes>> innerGetTapChangerSteps(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<OwnerInfo, List<TapChangerStepAttributes>> map = new HashMap<>();
            while (resultSet.next()) {

                OwnerInfo owner = new OwnerInfo();
                // In order, from the QueryCatalog.buildTapChangerStepQuery SQL query :
                // equipmentId, equipmentType, networkUuid, variantNum, "side", "tapChangerType", "rho", "r", "x", "g", "b", "alpha"
                owner.setEquipmentId(resultSet.getString(1));
                owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
                owner.setNetworkUuid(resultSet.getObject(3, UUID.class));
                owner.setVariantNum(resultSet.getInt(4));

                TapChangerStepAttributes tapChangerStep = new TapChangerStepAttributes();
                if (TapChangerType.valueOf(resultSet.getString(7)) == TapChangerType.RATIO) {
                    tapChangerStep.setType(TapChangerType.RATIO);
                } else {
                    tapChangerStep.setType(TapChangerType.PHASE);
                    tapChangerStep.setAlpha(resultSet.getDouble(13));
                }
                tapChangerStep.setIndex(resultSet.getInt(5));
                tapChangerStep.setSide(resultSet.getInt(6));
                tapChangerStep.setRho(resultSet.getDouble(8));
                tapChangerStep.setR(resultSet.getDouble(9));
                tapChangerStep.setX(resultSet.getDouble(10));
                tapChangerStep.setG(resultSet.getDouble(11));
                tapChangerStep.setB(resultSet.getDouble(12));

                map.computeIfAbsent(owner, k -> new ArrayList<>());
                map.get(owner).add(tapChangerStep);
            }
            return map;
        }
    }

    private <T extends IdentifiableAttributes> List<TapChangerStepAttributes> getTapChangerSteps(T equipment) {
        if (equipment instanceof TwoWindingsTransformerAttributes) {
            return getTapChangerStepsTwoWindingsTransformer((TwoWindingsTransformerAttributes) equipment);
        }
        if (equipment instanceof ThreeWindingsTransformerAttributes) {
            return getTapChangerStepsThreeWindingsTransformer((ThreeWindingsTransformerAttributes) equipment);
        }
        throw new UnsupportedOperationException("equipmentAttributes type invalid");
    }

    private List<TapChangerStepAttributes> getTapChangerStepsTwoWindingsTransformer(TwoWindingsTransformerAttributes equipment) {
        List<TapChangerStepAttributes> steps = new ArrayList<>();
        steps.addAll(getTapChangerStepsFromTapChangerAttributes(equipment.getRatioTapChangerAttributes(), 0, TapChangerType.RATIO));
        steps.addAll(getTapChangerStepsFromTapChangerAttributes(equipment.getPhaseTapChangerAttributes(), 0, TapChangerType.PHASE));
        return steps;
    }

    private List<TapChangerStepAttributes> getTapChangerStepsThreeWindingsTransformer(ThreeWindingsTransformerAttributes equipment) {
        List<TapChangerStepAttributes> steps = new ArrayList<>();
        for (Integer legNum : Set.of(1, 2, 3)) {
            steps.addAll(getTapChangerStepsFromTapChangerAttributes(equipment.getLeg(legNum).getRatioTapChangerAttributes(), legNum, TapChangerType.RATIO));
            steps.addAll(getTapChangerStepsFromTapChangerAttributes(equipment.getLeg(legNum).getPhaseTapChangerAttributes(), legNum, TapChangerType.PHASE));
        }
        return steps;
    }

    private List<TapChangerStepAttributes> getTapChangerStepsFromTapChangerAttributes(TapChangerAttributes tapChanger, Integer side, TapChangerType type) {
        if (tapChanger != null && tapChanger.getSteps() != null) {
            List<TapChangerStepAttributes> steps = tapChanger.getSteps();
            for (int i = 0; i < steps.size(); i++) {
                steps.get(i).setIndex(i);
                steps.get(i).setSide(side);
                steps.get(i).setType(type);
            }
            return steps;
        }
        return Collections.emptyList();
    }

    private <T extends IdentifiableAttributes>
        Map<OwnerInfo, List<TapChangerStepAttributes>> getTapChangerStepsFromEquipment(UUID networkUuid, List<Resource<T>> resources) {
        if (!resources.isEmpty()) {
            Map<OwnerInfo, List<TapChangerStepAttributes>> map = new HashMap<>();
            for (Resource<T> resource : resources) {
                T equipment = resource.getAttributes();
                List<TapChangerStepAttributes> steps = getTapChangerSteps(equipment);
                if (!steps.isEmpty()) {
                    OwnerInfo info = new OwnerInfo(
                        resource.getId(),
                        resource.getType(),
                        networkUuid,
                        resource.getVariantNum()
                    );
                    map.put(info, steps);
                }
            }
            return map;
        }
        return Collections.emptyMap();
    }

    private <T extends TapChangerStepAttributes>
        void insertTapChangerSteps(Map<OwnerInfo, List<T>> tapChangerSteps) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildInsertTapChangerStepQuery())) {
                List<Object> values = new ArrayList<>(13);

                List<Map.Entry<OwnerInfo, List<T>>> list = new ArrayList<>(tapChangerSteps.entrySet());
                for (List<Map.Entry<OwnerInfo, List<T>>> subTapChangerSteps : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<T>> entry : subTapChangerSteps) {
                        for (T tapChangerStep : entry.getValue()) {
                            values.clear();
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(tapChangerStep.getIndex());
                            values.add(tapChangerStep.getSide());
                            values.add(tapChangerStep.getType().toString());
                            values.add(tapChangerStep.getRho());
                            values.add(tapChangerStep.getR());
                            values.add(tapChangerStep.getX());
                            values.add(tapChangerStep.getG());
                            values.add(tapChangerStep.getB());
                            values.add(tapChangerStep.getAlpha());
                            bindValues(preparedStmt, values, mapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    protected <T extends IdentifiableAttributes>
        void insertTapChangerStepsInEquipments(UUID networkUuid, List<Resource<T>> equipments, Map<OwnerInfo, List<TapChangerStepAttributes>> tapChangerSteps) {

        if (tapChangerSteps.isEmpty() || equipments.isEmpty()) {
            return;
        }
        for (Resource<T> equipmentAttributesResource : equipments) {
            OwnerInfo owner = new OwnerInfo(
                equipmentAttributesResource.getId(),
                equipmentAttributesResource.getType(),
                networkUuid,
                equipmentAttributesResource.getVariantNum()
            );
            if (!tapChangerSteps.containsKey(owner)) {
                continue;
            }

            T equipment = equipmentAttributesResource.getAttributes();
            if (equipment instanceof TwoWindingsTransformerAttributes) {
                for (TapChangerStepAttributes tapChangerStep : tapChangerSteps.get(owner)) {
                    insertTapChangerStepInEquipment((TwoWindingsTransformerAttributes) equipment, tapChangerStep);
                }
            } else if (equipment instanceof ThreeWindingsTransformerAttributes) {
                for (TapChangerStepAttributes tapChangerStep : tapChangerSteps.get(owner)) {
                    LegAttributes leg = ((ThreeWindingsTransformerAttributes) equipment).getLeg(tapChangerStep.getSide());
                    insertTapChangerStepInEquipment(leg, tapChangerStep);
                }
            }
        }
    }

    private <T extends TapChangerParentAttributes>
        void insertTapChangerStepInEquipment(T tapChangerParent, TapChangerStepAttributes tapChangerStep) {
        if (tapChangerStep == null) {
            return;
        }
        TapChangerType type = tapChangerStep.getType();

        if (type == TapChangerType.RATIO) {
            if (tapChangerParent.getRatioTapChangerAttributes() == null) {
                tapChangerParent.setRatioTapChangerAttributes(new RatioTapChangerAttributes());
            }
            if (tapChangerParent.getRatioTapChangerAttributes().getSteps() == null) {
                tapChangerParent.getRatioTapChangerAttributes().setSteps(new ArrayList<>());
            }
            tapChangerParent.getRatioTapChangerAttributes().getSteps().add(tapChangerStep); // check side value here ?
        } else { // PHASE
            if (tapChangerParent.getPhaseTapChangerAttributes() == null) {
                tapChangerParent.setPhaseTapChangerAttributes(new PhaseTapChangerAttributes());
            }
            if (tapChangerParent.getPhaseTapChangerAttributes().getSteps() == null) {
                tapChangerParent.getPhaseTapChangerAttributes().setSteps(new ArrayList<>());
            }
            tapChangerParent.getPhaseTapChangerAttributes().getSteps().add(tapChangerStep);
        }
    }

    private void deleteTapChangerSteps(UUID networkUuid, int variantNum, String equipmentId) {
        deleteTapChangerSteps(networkUuid, variantNum, List.of(equipmentId));
    }

    private void deleteTapChangerSteps(UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryCatalog.buildDeleteTapChangerStepVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setObject(1, networkUuid);
                preparedStmt.setInt(2, variantNum);
                for (int i = 0; i < equipmentIds.size(); i++) {
                    preparedStmt.setString(3 + i, equipmentIds.get(i));
                }
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private <T extends IdentifiableAttributes>
        void deleteTapChangerSteps(UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, List<String>> resourceIdsByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            List<String> resourceIds = resourceIdsByVariant.get(resource.getVariantNum());
            if (resourceIds != null) {
                resourceIds.add(resource.getId());
            } else {
                resourceIds = new ArrayList<>();
                resourceIds.add(resource.getId());
            }
            resourceIdsByVariant.put(resource.getVariantNum(), resourceIds);
        }
        resourceIdsByVariant.forEach((k, v) -> deleteTapChangerSteps(networkUuid, k, v));
    }

    private static final String EXCEPTION_UNKNOWN_LIMIT_TYPE = "Unknown limit type";

    private void fillLimitsInfosByTypeAndSide(LimitHolder equipment, LimitsInfos result, LimitType type, int side) {
        Map<String, OperationalLimitsGroupAttributes> operationalLimitsGroups = equipment.getOperationalLimitsGroups(side);
        if (operationalLimitsGroups != null) {
            for (Map.Entry<String, OperationalLimitsGroupAttributes> entry : operationalLimitsGroups.entrySet()) {
                LimitsAttributes limits = getLimits(equipment, type, side, entry.getKey());
                if (limits != null) {
                    if (limits.getTemporaryLimits() != null) {
                        List<TemporaryLimitAttributes> temporaryLimits = new ArrayList<>(
                                limits.getTemporaryLimits().values());
                        temporaryLimits.forEach(e -> {
                            e.setSide(side);
                            e.setLimitType(type);
                            e.setOperationalLimitsGroupId(entry.getKey());
                        });
                        result.getTemporaryLimits().addAll(temporaryLimits);
                    }
                    if (!Double.isNaN(limits.getPermanentLimit())) {
                        result.getPermanentLimits().add(PermanentLimitAttributes.builder()
                                .side(side)
                                .limitType(type)
                                .value(limits.getPermanentLimit())
                                .operationalLimitsGroupId(entry.getKey())
                                .build());
                    }
                }
            }
        }
    }

    private LimitsInfos getAllLimitsInfos(LimitHolder equipment) {
        LimitsInfos result = new LimitsInfos();
        for (Integer side : equipment.getSideList()) {
            fillLimitsInfosByTypeAndSide(equipment, result, LimitType.CURRENT, side);
            fillLimitsInfosByTypeAndSide(equipment, result, LimitType.ACTIVE_POWER, side);
            fillLimitsInfosByTypeAndSide(equipment, result, LimitType.APPARENT_POWER, side);
        }
        return result;
    }

    private void setLimits(LimitHolder equipment, LimitType type, int side, LimitsAttributes limits, String operationalLimitsGroupId) {
        switch (type) {
            case CURRENT -> equipment.setCurrentLimits(side, limits, operationalLimitsGroupId);
            case APPARENT_POWER -> equipment.setApparentPowerLimits(side, limits, operationalLimitsGroupId);
            case ACTIVE_POWER -> equipment.setActivePowerLimits(side, limits, operationalLimitsGroupId);
            default -> throw new IllegalArgumentException(EXCEPTION_UNKNOWN_LIMIT_TYPE);
        }
    }

    private LimitsAttributes getLimits(LimitHolder equipment, LimitType type, int side, String operationalLimitsGroupId) {
        return switch (type) {
            case CURRENT -> equipment.getCurrentLimits(side, operationalLimitsGroupId);
            case APPARENT_POWER -> equipment.getApparentPowerLimits(side, operationalLimitsGroupId);
            case ACTIVE_POWER -> equipment.getActivePowerLimits(side, operationalLimitsGroupId);
            default -> throw new IllegalArgumentException(EXCEPTION_UNKNOWN_LIMIT_TYPE);
        };
    }
}
