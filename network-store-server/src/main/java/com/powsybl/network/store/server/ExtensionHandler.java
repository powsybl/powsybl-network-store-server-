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
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import org.springframework.stereotype.Component;

import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;

import static com.powsybl.network.store.server.NetworkStoreRepository.BATCH_SIZE;
import static com.powsybl.network.store.server.QueryCatalog.*;
import static com.powsybl.network.store.server.QueryExtensionCatalog.EXTENSION_NAME_COLUMN;
import static com.powsybl.network.store.server.Utils.bindValues;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@Component
public class ExtensionHandler {

    private final ObjectMapper mapper;

    public ExtensionHandler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void insertExtensions(Connection connection, Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions) throws SQLException {
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

    public Map<String, Set<String>> getTombstonedExtensions(Connection connection, UUID networkUuid, int variantNum) throws SQLException {
        Map<String, Set<String>> tombstonedExtensions = new HashMap<>();

        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetTombstonedExtensionsQuery())) {
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
        }

        return tombstonedExtensions;
    }

    public Optional<ExtensionAttributes> getExtensionAttributes(
            Connection connection,
            UUID networkId,
            int variantNum,
            String identifiableId,
            String extensionName,
            int fullVariantNum,
            Supplier<Set<String>> tombstonedIdsSupplier) throws SQLException {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve extensions for the specified variant directly
            return getExtensionAttributesForVariant(connection, networkId, variantNum, identifiableId, extensionName);
        }

        // Retrieve extension in partial variant
        Optional<ExtensionAttributes> partialVariantExtensionAttributes = getExtensionAttributesForVariant(connection, networkId, variantNum, identifiableId, extensionName);
        if (partialVariantExtensionAttributes.isPresent()) {
            return partialVariantExtensionAttributes;
        }

        // Return empty if it's a tombstoned extension or identifiable
        Map<String, Set<String>> tombstonedExtensions = getTombstonedExtensions(connection, networkId, variantNum);
        boolean isTombstonedExtension = tombstonedExtensions.containsKey(identifiableId) && tombstonedExtensions.get(identifiableId).contains(extensionName);
        Set<String> tombstonedIds = tombstonedIdsSupplier.get();
        boolean isTombstonedIdentifiable = tombstonedIds.contains(identifiableId);
        if (isTombstonedIdentifiable || isTombstonedExtension) {
            return Optional.empty();
        }

        // Retrieve extension from the full variant
        return getExtensionAttributesForVariant(connection, networkId, fullVariantNum, identifiableId, extensionName);
    }

    public Optional<ExtensionAttributes> getExtensionAttributesForVariant(Connection connection, UUID networkUuid, int variantNum, String identifiableId, String extensionName) {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetExtensionsQuery());) {
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

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByResourceTypeAndExtensionName(
            Connection connection,
            UUID networkId,
            int variantNum,
            String resourceType,
            String extensionName,
            int fullVariantNum,
            Supplier<Set<String>> tombstonedIdsSupplier) throws SQLException {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve extensions for the specified variant directly
            return getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(connection, networkId, variantNum, resourceType, extensionName);
        }

        // Retrieve extensions in full variant
        Map<String, ExtensionAttributes> extensionsAttributesByResourceTypeAndExtensionName = getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(connection, networkId, fullVariantNum, resourceType, extensionName);

        // Remove tombstoned identifiables and tombstoned extensions
        Set<String> tombstonedIds = tombstonedIdsSupplier.get();
        Map<String, Set<String>> tombstonedExtensions = getTombstonedExtensions(connection, networkId, variantNum);
        extensionsAttributesByResourceTypeAndExtensionName.entrySet().removeIf(entry ->
                        tombstonedIds.contains(entry.getKey()) ||
                        tombstonedExtensions.getOrDefault(entry.getKey(), Set.of()).contains(extensionName));

        // Retrieve extensions in partial variant
        Map<String, ExtensionAttributes> partialVariantExtensionsAttributesByResourceTypeAndExtensionName = getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(connection, networkId, variantNum, resourceType, extensionName);

        // Combine extensions from full and partial variants
        extensionsAttributesByResourceTypeAndExtensionName.putAll(partialVariantExtensionsAttributesByResourceTypeAndExtensionName);
        return extensionsAttributesByResourceTypeAndExtensionName;
    }

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(Connection connection, UUID networkUuid, int variantNum, String resourceType, String extensionName) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByResourceTypeAndExtensionName())) {
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, resourceType);
            preparedStmt.setString(4, extensionName);
            return innerGetAllExtensionsAttributesByResourceTypeAndExtensionName(preparedStmt);
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

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByIdentifiableId(
            Connection connection,
            UUID networkId,
            int variantNum,
            String identifiableId,
            int fullVariantNum,
            Supplier<Set<String>> tombstonedIdsSupplier) throws SQLException {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve extensions for the specified variant directly
            return getAllExtensionsAttributesByIdentifiableIdForVariant(connection, networkId, variantNum, identifiableId);
        }

        Set<String> tombstonedIds = tombstonedIdsSupplier.get();
        if (tombstonedIds.contains(identifiableId)) {
            // If the identifiable is tombstoned, we return directly
            return Map.of();
        }

        // Retrieve extensions from full variant
        Map<String, ExtensionAttributes> extensionsAttributesByIdentifiableId = getAllExtensionsAttributesByIdentifiableIdForVariant(connection, networkId, fullVariantNum, identifiableId);

        // Remove tombstoned extensions
        Map<String, Set<String>> tombstonedExtensions = getTombstonedExtensions(connection, networkId, variantNum);
        extensionsAttributesByIdentifiableId.entrySet().removeIf(entry -> tombstonedExtensions.getOrDefault(identifiableId, Set.of()).contains(entry.getKey()));

        // Retrieve extensions in partial variant
        Map<String, ExtensionAttributes> partialVariantExtensionsAttributesByResourceTypeAndExtensionName = getAllExtensionsAttributesByIdentifiableIdForVariant(connection, networkId, variantNum, identifiableId);

        // Combine extensions from full and partial variants
        extensionsAttributesByIdentifiableId.putAll(partialVariantExtensionsAttributesByResourceTypeAndExtensionName);
        return extensionsAttributesByIdentifiableId;
    }

    public Map<String, ExtensionAttributes> getAllExtensionsAttributesByIdentifiableIdForVariant(Connection connection, UUID networkUuid, int variantNum, String identifiableId) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByIdentifiableId())) {
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, identifiableId);
            return innerGetAllExtensionsAttributesByIdentifiableId(preparedStmt);
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

    public Map<String, Map<String, ExtensionAttributes>> getAllExtensionsAttributesByResourceType(
            Connection connection, UUID networkId,
            int variantNum,
            ResourceType type,
            int fullVariantNum,
            Supplier<Set<String>> tombstonedIdsSupplier) throws SQLException {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve extensions for the specified variant directly
            return getAllExtensionsAttributesByResourceTypeForVariant(connection, networkId, variantNum, type.toString());
        }

        // Retrieve extensions from full variant
        Map<String, Map<String, ExtensionAttributes>> extensionsAttributesByResourceType = getAllExtensionsAttributesByResourceTypeForVariant(connection, networkId, fullVariantNum, type.toString());

        // Remove tombstoned identifiables
        Set<String> tombstonedIds = tombstonedIdsSupplier.get();
        extensionsAttributesByResourceType.keySet().removeIf(tombstonedIds::contains);

        // Remove tombstoned extensions
        Map<String, Set<String>> tombstonedExtensions = getTombstonedExtensions(connection, networkId, variantNum);
        extensionsAttributesByResourceType.forEach((identifiableId, extensionsAttributes) -> {
            Set<String> tombstonedExtensionNames = tombstonedExtensions.get(identifiableId);
            if (tombstonedExtensionNames != null) {
                extensionsAttributes.keySet().removeIf(tombstonedExtensionNames::contains);
            }
        });
        // Remove entries with no remaining extensions after removing tombstoned extensions
        extensionsAttributesByResourceType.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Retrieve extensions in partial variant
        Map<String, Map<String, ExtensionAttributes>> partialVariantExtensionsAttributesByResourceType = getAllExtensionsAttributesByResourceTypeForVariant(connection, networkId, variantNum, type.toString());

        // Combine extensions from full and partial variants
        partialVariantExtensionsAttributesByResourceType.forEach((identifiableId, updatedExtensions) ->
                extensionsAttributesByResourceType.merge(identifiableId, updatedExtensions, (existingExtensions, newExtensions) -> {
                    // Merge each extension within the nested maps
                    newExtensions.forEach((extensionName, newExtensionAttributes) ->
                            existingExtensions.merge(extensionName, newExtensionAttributes, (oldValue, value) -> value));
                    return existingExtensions;
                })
        );
        return extensionsAttributesByResourceType;
    }

    public Map<String, Map<String, ExtensionAttributes>> getAllExtensionsAttributesByResourceTypeForVariant(Connection connection, UUID networkUuid, int variantNum, String resourceType) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildGetAllExtensionsAttributesByResourceType())) {
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            preparedStmt.setString(3, resourceType);
            return innerGetAllExtensionsAttributesByResourceType(preparedStmt);
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

    public void deleteExtensionsFromIdentifiable(Connection connection, UUID networkUuid, int variantNum, String equipmentId) throws SQLException {
        deleteExtensionsFromIdentifiables(connection, networkUuid, variantNum, List.of(equipmentId));
    }

    public void deleteExtensionsFromIdentifiables(Connection connection, UUID networkUuid, int variantNum, List<String> equipmentIds) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildDeleteExtensionsVariantEquipmentINQuery(equipmentIds.size()))) {
            preparedStmt.setObject(1, networkUuid);
            preparedStmt.setInt(2, variantNum);
            for (int i = 0; i < equipmentIds.size(); i++) {
                preparedStmt.setString(3 + i, equipmentIds.get(i));
            }
            preparedStmt.executeUpdate();
        }
    }

    public void deleteExtensionsFromIdentifiables(Connection connection, UUID networkUuid, int variantNum, Map<String, Set<String>> extensionNamesByIdentifiableId) {
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
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }
    }

    public void deleteAndTombstoneExtensions(Connection connection, UUID networkUuid, int variantNum, Map<String, Set<String>> extensionNamesByIdentifiableId, boolean isPartialVariant) throws SQLException {
        deleteExtensionsFromIdentifiables(connection, networkUuid, variantNum, extensionNamesByIdentifiableId);
        if (isPartialVariant) {
            insertTombstonedExtensions(networkUuid, variantNum, extensionNamesByIdentifiableId, connection);
        }
    }

    public void insertTombstonedExtensions(UUID networkId, int variantNum, Map<String, Set<String>> extensionNamesByIdentifiableId, Connection connection) throws SQLException {
        try (var preparedStmt = connection.prepareStatement(QueryExtensionCatalog.buildInsertTombstonedExtensionsQuery())) {
            for (Map.Entry<String, Set<String>> entry : extensionNamesByIdentifiableId.entrySet()) {
                String identifiableId = entry.getKey();
                for (String extensionName : entry.getValue()) {
                    preparedStmt.setObject(1, networkId);
                    preparedStmt.setInt(2, variantNum);
                    preparedStmt.setString(3, identifiableId);
                    preparedStmt.setString(4, extensionName);
                    preparedStmt.addBatch();
                }
            }
            preparedStmt.executeBatch();
        }
    }

    /**
     * Delete extension attributes loaded in the resource and insert the updated extension afterward.
     * We can't delete all the extensions attributes associated with this resource here as we are not sure that all
     * extension attributes have been modified/loaded in the resource.
     */
    public <T extends IdentifiableAttributes> void updateExtensionsFromEquipments(Connection connection, UUID networkUuid, List<Resource<T>> resources) throws SQLException {
        deleteExtensionsFromEquipments(connection, networkUuid, resources);
        insertExtensions(connection, getExtensionsFromEquipments(networkUuid, resources));
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

    public <T extends IdentifiableAttributes> void deleteExtensionsFromEquipments(Connection connection, UUID networkUuid, List<Resource<T>> resources) {
        Map<Integer, Map<String, Set<String>>> extensionsAttributesByVariant = new HashMap<>();
        for (Resource<T> resource : resources) {
            extensionsAttributesByVariant
                    .computeIfAbsent(resource.getVariantNum(), k -> new HashMap<>())
                    .put(resource.getId(), resource.getAttributes().getExtensionAttributes().keySet());
        }
        extensionsAttributesByVariant.forEach((k, v) -> deleteExtensionsFromIdentifiables(connection, networkUuid, k, v));
    }

    /**
     * Delete extension attributes loaded in the resource and insert the updated extension afterward.
     * We can't delete all the extensions attributes associated with this resource here as we are not sure that all
     * extension attributes have been modified/loaded in the resource.
     */
    public void updateExtensionsFromNetworks(Connection connection, List<Resource<NetworkAttributes>> resources) throws SQLException {
        deleteExtensionsFromNetworks(connection, resources);
        insertExtensions(connection, getExtensionsFromNetworks(resources));
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

    private void deleteExtensionsFromNetworks(Connection connection, List<Resource<NetworkAttributes>> resources) {
        for (Resource<NetworkAttributes> resource : resources) {
            deleteExtensionsFromIdentifiables(connection, resource.getAttributes().getUuid(), resource.getVariantNum(), Map.of(resource.getId(), resource.getAttributes().getExtensionAttributes().keySet()));
        }
    }
}
