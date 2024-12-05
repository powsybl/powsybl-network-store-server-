/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.powsybl.network.store.model.ExtensionAttributes;
import com.powsybl.network.store.model.IdentifiableAttributes;
import com.powsybl.network.store.model.NetworkAttributes;
import com.powsybl.network.store.model.Resource;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.powsybl.network.store.server.NetworkStoreRepository.BATCH_SIZE;
import static com.powsybl.network.store.server.QueryCatalog.*;
import static com.powsybl.network.store.server.QueryExtensionCatalog.EXTENSION_NAME_COLUMN;
import static com.powsybl.network.store.server.Utils.bindValues;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@Component
public class ExtensionHandler {
    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public ExtensionHandler(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public void createExtensions(Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions) {
        Map<OwnerInfo, Map<String, ExtensionAttributes>> filteredExtensions = extensions.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (filteredExtensions.isEmpty()) {
            return;
        }

        try (var connection = dataSource.getConnection()) {
            insertExtensions(filteredExtensions, connection);
            List<Object> values = new ArrayList<>(filteredExtensions.size() * 4);
            List<Map.Entry<OwnerInfo, Map<String, ExtensionAttributes>>> list = new ArrayList<>(filteredExtensions.entrySet());
            for (Map.Entry<OwnerInfo, Map<String, ExtensionAttributes>> entry : list) {
                for (Map.Entry<String, ExtensionAttributes> extension : entry.getValue().entrySet()) {
                    values.add(entry.getKey().getNetworkUuid());
                    values.add(entry.getKey().getVariantNum());
                    values.add(entry.getKey().getEquipmentId());
                    values.add(extension.getKey());
                }
            }

            try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteTombstonedExtensionsQuery(values.size() / 4))) {
                bindValues(preparedStmt, values, mapper);
                preparedStmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void recreateExtensionsForUpdate(Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions) {
        try (var connection = dataSource.getConnection()) {
            insertExtensions(extensions, connection);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void insertExtensions(Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions, Connection connection) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildInsertExtensionsQuery())) {
            List<Object> values = new ArrayList<>(6);
            List<Map.Entry<OwnerInfo, Map<String, ExtensionAttributes>>> list = new ArrayList<>(extensions.entrySet());
            for (List<Map.Entry<OwnerInfo, Map<String, ExtensionAttributes>>> subExtensions : Lists.partition(list, BATCH_SIZE)) {
                for (Map.Entry<OwnerInfo, Map<String, ExtensionAttributes>> entry : subExtensions) {
                    for (Map.Entry<String, ExtensionAttributes> extension : entry.getValue().entrySet()) {
                        if (extension.getValue().isPersistent()) {
                            values.clear();
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(extension.getKey());
                            values.add(extension.getValue());
                            bindValues(preparedStmt, values, mapper);
                            preparedStmt.addBatch();
                        }
                    }
                }
                preparedStmt.executeBatch();
            }
        }
    }

    public Map<String, Set<String>> getTombstonedExtensions(UUID networkUuid, int variantNum) {
        Map<String, Set<String>> tombstonedExtensions = new HashMap<>();

        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetTombstonedExtensionsQuery());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);

            try (var resultSet = preparedStmt.executeQuery()) {
                while (resultSet.next()) {
                    String identifiableId = resultSet.getString(EQUIPMENT_ID_COLUMN);
                    String extensionName = resultSet.getString(EXTENSION_NAME_COLUMN);

                    tombstonedExtensions
                            .computeIfAbsent(identifiableId, k -> new HashSet<>())
                            .add(extensionName);
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }

        return tombstonedExtensions;
    }

    public Optional<ExtensionAttributes> getExtensionAttributes(UUID networkUuid, int variantNum, String identifiableId, String extensionName) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetExtensionsQuery());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, identifiableId);
            preparedStmt.setString(4, extensionName);

            return innerGetExtensionAttributes(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Optional<ExtensionAttributes> innerGetExtensionAttributes(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            if (resultSet.next()) {
                return Optional.of(mapper.readValue(resultSet.getString(1), ExtensionAttributes.class));
            }
            return Optional.empty();
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByResourceTypeAndExtensionName(UUID networkUuid, int variantNum, String resourceType, String extensionName) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByResourceTypeAndExtensionName());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, resourceType);
            preparedStmt.setString(4, extensionName);
            return innerGetAllExtensionsAttributesByResourceTypeAndExtensionName(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<String, ExtensionAttributes> innerGetAllExtensionsAttributesByResourceTypeAndExtensionName(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<String, ExtensionAttributes> map = new HashMap<>();
            while (resultSet.next()) {
                String equipmentId = resultSet.getString(1);
                ExtensionAttributes extensionValue = mapper.readValue(resultSet.getString(2), ExtensionAttributes.class);
                map.put(equipmentId, extensionValue);
            }
            return map;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByIdentifiableId(UUID networkUuid, int variantNum, String identifiableId) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByIdentifiableId());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, identifiableId);
            return innerGetAllExtensionsAttributesByIdentifiableId(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<String, ExtensionAttributes> innerGetAllExtensionsAttributesByIdentifiableId(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<String, ExtensionAttributes> map = new HashMap<>();
            while (resultSet.next()) {
                String extensionName = resultSet.getString(1);
                ExtensionAttributes extensionValue = mapper.readValue(resultSet.getString(2), ExtensionAttributes.class);
                map.put(extensionName, extensionValue);
            }
            return map;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, Map<String, ExtensionAttributes>> getAllExtensionsAttributesByResourceType(UUID networkUuid, int variantNum, String resourceType) {
        try (var connection = dataSource.getConnection()) {
            var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByResourceType());
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, resourceType);
            return innerGetAllExtensionsAttributesByResourceType(preparedStmt);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Map<String, Map<String, ExtensionAttributes>> innerGetAllExtensionsAttributesByResourceType(PreparedStatement preparedStmt) throws SQLException {
        try (ResultSet resultSet = preparedStmt.executeQuery()) {
            Map<String, Map<String, ExtensionAttributes>> map = new HashMap<>();
            while (resultSet.next()) {
                String equipmentId = resultSet.getString(1);
                String extensionName = resultSet.getString(2);
                ExtensionAttributes extensionValue = mapper.readValue(resultSet.getString(3), ExtensionAttributes.class);
                map.computeIfAbsent(equipmentId, k -> new HashMap<>()).put(extensionName, extensionValue);
            }
            return map;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteExtensionsFromIdentifiable(UUID networkUuid, int variantNum, String equipmentId) {
        deleteExtensionsFromIdentifiables(networkUuid, variantNum, List.of(equipmentId));
    }

    public void deleteExtensionsFromIdentifiables(UUID networkUuid, int variantNum, List<String> equipmentIds) {
        try (var connection = dataSource.getConnection()) {
            try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteExtensionsVariantEquipmentINQuery(equipmentIds.size()))) {
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

    public void deleteExtensionsFromIdentifiables(UUID networkUuid, int variantNum, Map<String, Set<String>> extensionNamesByIdentifiableId) {
        try (var connection = dataSource.getConnection()) {
            int totalParameters = extensionNamesByIdentifiableId.values().stream().mapToInt(Set::size).sum();
            if (totalParameters > 0) {
                try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteExtensionsVariantByIdentifiableIdAndExtensionsNameINQuery(totalParameters))) {
                    preparedStmt.setObject(1, networkUuid);
                    preparedStmt.setInt(2, variantNum);

                    int paramIndex = 3;
                    for (Map.Entry<String, Set<String>> entry : extensionNamesByIdentifiableId.entrySet()) {
                        String equipmentId = entry.getKey();
                        for (String extensionName : entry.getValue()) {
                            preparedStmt.setString(paramIndex++, equipmentId);
                            preparedStmt.setString(paramIndex++, extensionName);
                        }
                    }

                    preparedStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    /**
     * Delete extension attributes loaded in the resource and insert the updated extension afterward.
     * We can't delete all the extensions attributes associated with this resource here as we are not sure that all
     * extension attributes have been modified/loaded in the resource.
     */
    public <T extends IdentifiableAttributes> void updateExtensionsFromEquipments(UUID networkUuid, List<Resource<T>> resources) {
        deleteExtensionsFromEquipments(networkUuid, resources);
        recreateExtensionsForUpdate(getExtensionsFromEquipments(networkUuid, resources));
    }

    public <T extends IdentifiableAttributes> Map<OwnerInfo, Map<String, ExtensionAttributes>> getExtensionsFromEquipments(UUID networkUuid, List<Resource<T>> resources) {
        Map<OwnerInfo, Map<String, ExtensionAttributes>> map = new HashMap<>();

        if (!resources.isEmpty()) {
            for (Resource<T> resource : resources) {
                Map<String, ExtensionAttributes> extensions = resource.getAttributes().getExtensionAttributes();
                if (extensions != null && !extensions.isEmpty()) {
                    OwnerInfo info = new OwnerInfo(
                            resource.getId(),
                            resource.getType(),
                            networkUuid,
                            resource.getVariantNum()
                    );
                    map.put(info, extensions);
                }
            }
        }
        return map;
    }

    public <T extends IdentifiableAttributes> void deleteExtensionsFromEquipments(UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, Map<String, Set<String>>> extensionsAttributesByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            extensionsAttributesByVariant
                    .computeIfAbsent(resource.getVariantNum(), k -> new HashMap<>())
                    .put(resource.getId(), resource.getAttributes().getExtensionAttributes().keySet());
        }
        extensionsAttributesByVariant.forEach((k, v) -> deleteExtensionsFromIdentifiables(networkUuid, k, v));
    }

    /**
     * Delete extension attributes loaded in the resource and insert the updated extension afterward.
     * We can't delete all the extensions attributes associated with this resource here as we are not sure that all
     * extension attributes have been modified/loaded in the resource.
     */
    public void updateExtensionsFromNetworks(List<Resource<NetworkAttributes>> resources) {
        deleteExtensionsFromNetworks(resources);
        recreateExtensionsForUpdate(getExtensionsFromNetworks(resources));
    }

    public Map<OwnerInfo, Map<String, ExtensionAttributes>> getExtensionsFromNetworks(List<Resource<NetworkAttributes>> resources) {
        Map<OwnerInfo, Map<String, ExtensionAttributes>> map = new HashMap<>();

        if (!resources.isEmpty()) {
            for (Resource<NetworkAttributes> resource : resources) {
                Map<String, ExtensionAttributes> extensions = resource.getAttributes().getExtensionAttributes();
                if (extensions != null && !extensions.isEmpty()) {
                    OwnerInfo info = new OwnerInfo(
                            resource.getId(),
                            resource.getType(),
                            resource.getAttributes().getUuid(),
                            resource.getVariantNum()
                    );
                    map.put(info, extensions);
                }
            }
        }
        return map;
    }

    private void deleteExtensionsFromNetworks(List<Resource<NetworkAttributes>> resources) {
        for (Resource<NetworkAttributes> resource : resources) {
            deleteExtensionsFromIdentifiables(resource.getAttributes().getUuid(), resource.getVariantNum(), Map.of(resource.getId(), resource.getAttributes().getExtensionAttributes().keySet()));
        }
    }
}
