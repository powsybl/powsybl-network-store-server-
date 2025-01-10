/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.network.store.model.ResourceType;
import com.powsybl.network.store.model.TemporaryLimitAttributes;
import com.powsybl.network.store.model.VariantInfos;
import com.powsybl.network.store.server.ExtensionHandler;
import com.powsybl.network.store.server.Mappings;
import com.powsybl.network.store.server.NetworkStoreRepository;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.powsybl.network.store.server.QueryCatalog.EQUIPMENT_TYPE_COLUMN;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
public class V211LimitsMigration implements CustomTaskChange {

    private static final Logger LOGGER = LoggerFactory.getLogger(V211LimitsMigration.class);

    private NetworkStoreRepository repository;

    public void init(Database database) {
        DataSource dataSource = new SingleConnectionDataSource(((JdbcConnection) database.getConnection()).getUnderlyingConnection(), true);
        ObjectMapper mapper = new ObjectMapper();
        this.repository = new NetworkStoreRepository(dataSource, mapper, new Mappings(), new ExtensionHandler(dataSource, mapper));
    }

    @Override
    public void execute(Database database) {
        init(database);
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement("select distinct uuid from network ")) {
            ResultSet networkUuids = stmt.executeQuery();
            while (networkUuids.next()) {
                UUID networkUuid = UUID.fromString(networkUuids.getString(1));
                migrateV211Limits(repository, networkUuid);
            }
        } catch (SQLException | DatabaseException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "V2.11 limits were successfully migrated";
    }

    @Override
    public void setUp() {
        LOGGER.info("Set up migration for limits");
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        LOGGER.info("Set file opener for limits migration");
    }

    @Override
    public ValidationErrors validate(Database database) {
        return new ValidationErrors();
    }

    // Methods to migrate V2.11 limits — do not use them in application code
    // TODO: All the methods below should be deprecated when limits are fully migrated — should be after v2.13 deployment

    public static Map<OwnerInfo, List<PermanentLimitAttributes>> getV211PermanentLimitsWithInClause(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (Connection connection = repository.getDataSource().getConnection()) {
            var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildGetV211PermanentLimitWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setString(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }

            return innerGetV211PermanentLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public static Map<OwnerInfo, List<TemporaryLimitAttributes>> getV211TemporaryLimits(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (Connection connection = repository.getDataSource().getConnection()) {
            var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildGetV211TemporaryLimitQuery(columnNameForWhereClause));
            preparedStmt.setString(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetV211TemporaryLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public static Map<OwnerInfo, List<PermanentLimitAttributes>> getV211PermanentLimits(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        try (Connection connection = repository.getDataSource().getConnection()) {
            var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildGetV211PermanentLimitQuery(columnNameForWhereClause));
            preparedStmt.setString(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, valueForWhereClause);

            return innerGetV211PermanentLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public static Map<OwnerInfo, List<TemporaryLimitAttributes>> getV211TemporaryLimitsWithInClause(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String columnNameForWhereClause, List<String> valuesForInClause) {
        if (valuesForInClause.isEmpty()) {
            return Collections.emptyMap();
        }
        try (Connection connection = repository.getDataSource().getConnection()) {
            var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildGetV211TemporaryLimitWithInClauseQuery(columnNameForWhereClause, valuesForInClause.size()));
            preparedStmt.setString(1, networkUuid.toString());
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < valuesForInClause.size(); i++) {
                preparedStmt.setString(3 + i, valuesForInClause.get(i));
            }

            return innerGetV211TemporaryLimits(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public static void migrateV211Limits(NetworkStoreRepository repository, UUID networkId) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        List<Integer> variantNums = repository.getVariantsInfos(networkId).stream().map(VariantInfos::getNum).toList();
        variantNums.forEach(variantNum -> {
            migrateV211Limits(repository, networkId, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.LINE.toString());
            migrateV211Limits(repository, networkId, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.TWO_WINDINGS_TRANSFORMER.toString());
            migrateV211Limits(repository, networkId, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.THREE_WINDINGS_TRANSFORMER.toString());
            migrateV211Limits(repository, networkId, variantNum, EQUIPMENT_TYPE_COLUMN, ResourceType.DANGLING_LINE.toString());
        });

        stopwatch.stop();
        LOGGER.info("Limits of network {} migrated in {} ms", networkId, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static void insertNewLimitsAndDeleteV211(NetworkStoreRepository repository, UUID networkUuid, int variantNum, Map<OwnerInfo, List<TemporaryLimitAttributes>> v211TemporaryLimits, Map<OwnerInfo, List<PermanentLimitAttributes>> v211PermanentLimits) {
        try (Connection connection = repository.getDataSource().getConnection()) {
            if (!v211PermanentLimits.keySet().isEmpty()) {
                repository.insertPermanentLimitsAttributes(v211PermanentLimits);
                deleteV211PermanentLimits(connection, networkUuid, variantNum, v211PermanentLimits.keySet().stream().map(OwnerInfo::getEquipmentId).toList());
            }
            if (!v211TemporaryLimits.keySet().isEmpty()) {
                repository.insertTemporaryLimitsAttributes(v211TemporaryLimits);
                deleteV211TemporaryLimits(connection, networkUuid, variantNum, v211TemporaryLimits.keySet().stream().map(OwnerInfo::getEquipmentId).toList());
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public static void migrateV211Limits(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String columnNameForWhereClause, String valueForWhereClause) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<OwnerInfo, List<TemporaryLimitAttributes>> v211TemporaryLimits = getV211TemporaryLimits(repository, networkUuid, variantNum, columnNameForWhereClause, valueForWhereClause);
        Map<OwnerInfo, List<PermanentLimitAttributes>> v211PermanentLimits = getV211PermanentLimits(repository, networkUuid, variantNum, columnNameForWhereClause, valueForWhereClause);
        insertNewLimitsAndDeleteV211(repository, networkUuid, variantNum, v211TemporaryLimits, v211PermanentLimits);
        stopwatch.stop();
        LOGGER.info("Limits of {}S of network {}/variantNum={} migrated in {} ms", valueForWhereClause, networkUuid, variantNum, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public static Map<OwnerInfo, List<TemporaryLimitAttributes>> innerGetV211TemporaryLimits(PreparedStatement preparedStmt) throws SQLException {
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

    public static Map<OwnerInfo, List<PermanentLimitAttributes>> innerGetV211PermanentLimits(PreparedStatement preparedStmt) throws SQLException {
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

    public static void deleteV211TemporaryLimits(Connection connection, UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try {
            try (var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildDeleteV211TemporaryLimitsVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setString(1, networkUuid.toString());
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

    public static void deleteV211PermanentLimits(Connection connection, UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try {
            try (var preparedStmt = connection.prepareStatement(V211LimitsQueryCatalog.buildDeleteV211PermanentLimitsVariantEquipmentINQuery(equipmentIds.size()))) {
                preparedStmt.setString(1, networkUuid.toString());
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
}
