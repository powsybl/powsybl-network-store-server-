/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.network.store.model.Attributes;
import com.powsybl.network.store.model.NetworkAttributes;
import com.powsybl.network.store.model.Resource;
import com.powsybl.network.store.server.dto.OwnerInfo;
import org.apache.commons.lang3.function.TriFunction;

import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
public final class PartialVariantUtils {
    private PartialVariantUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class can not be initialize.");
    }

    public static <T, U> Map<T, U> getExternalAttributes(
            int variantNum,
            int fullVariantNum,
            Supplier<Set<String>> fetchTombstonedExternalAttributesIds,
            Supplier<Set<String>> fetchTombstonedIdentifiableIds,
            IntFunction<Map<T, U>> fetchExternalAttributesInVariant,
            Function<T, String> idExtractor) {
        return getExternalAttributes(
                variantNum,
                fullVariantNum,
                fetchTombstonedExternalAttributesIds,
                fetchTombstonedIdentifiableIds,
                null,
                fetchExternalAttributesInVariant,
                idExtractor
        );
    }

    private static <T, U> Map<T, U> getExternalAttributes(
            int variantNum,
            int fullVariantNum,
            Supplier<Set<String>> fetchTombstonedExternalAttributesIdentifiableIds,
            Supplier<Set<String>> fetchTombstonedIdentifiableIds,
            Supplier<Set<String>> fetchAdditionalIdentifiableIdsToExclude,
            IntFunction<Map<T, U>> fetchExternalAttributesInVariant,
            Function<T, String> idExtractor) {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve external attributes directly
            return fetchExternalAttributesInVariant.apply(variantNum);
        }

        // Retrieve external attributes from the full variant first
        Map<T, U> externalAttributes = fetchExternalAttributesInVariant.apply(fullVariantNum);

        // Remove external attributes associated to tombstoned resources, tombstoned external attributes and any other additional identifiable ids
        Set<String> tombstonedIds = fetchTombstonedIdentifiableIds.get();
        Set<String> tombstonedExternalAttributesIds = fetchTombstonedExternalAttributesIdentifiableIds.get();
        Set<String> additionalIdentifiableIdsToExclude = fetchAdditionalIdentifiableIdsToExclude != null ? fetchAdditionalIdentifiableIdsToExclude.get() : Set.of();
        externalAttributes.keySet().removeIf(ownerInfo ->
                        tombstonedIds.contains(idExtractor.apply(ownerInfo)) ||
                        tombstonedExternalAttributesIds.contains(idExtractor.apply(ownerInfo)) ||
                        additionalIdentifiableIdsToExclude.contains(idExtractor.apply(ownerInfo))
        );

        // Retrieve external attributes in partial variant
        Map<T, U> partialVariantExternalAttributes = fetchExternalAttributesInVariant.apply(variantNum);

        // Combine external attributes from full and partial variant
        externalAttributes.putAll(partialVariantExternalAttributes);

        return externalAttributes;
    }

    /*
    Regulating equipments are derived from the regulating points table using a WHERE clause on the column `regulatedequipmenttype`.
    Due to this filtering, the system that overrides OwnerInfo in the full variant with updated regulating points in the
    partial variant does not behave as expected because of the additional WHERE clause.

    For example, consider a generator in the full variant with local regulation (`regulatedequipmenttype = generator`).
    If this is updated in the partiverifal variant to regulate a load (`regulatedequipmenttype = load`), calling `getRegulatingEquipments`
    with `type = generator` for the partial variant will yield unexpected results. The system will retrieve the
    regulation from the full variant (as it matches `regulatedequipmenttype = generator`).

    To address this inconsistency, an additional fetch is performed to exclude any regulating points in the full variant
    that have been updated in the partial variant. This ensures that the results reflect the changes made in the partial variant.
    */
    public static <T, U> Map<T, U> getRegulatingEquipments(
            int variantNum,
            int fullVariantNum,
            Supplier<Set<String>> fetchTombstonedRegulatingPointsIds,
            Supplier<Set<String>> fetchTombstonedIdentifiableIds,
            Supplier<Set<String>> fetchRegulatingPointIdsInVariant,
            IntFunction<Map<T, U>> fetchExternalAttributesInVariant,
            Function<T, String> idExtractor) {
        return getExternalAttributes(
                variantNum,
                fullVariantNum,
                fetchTombstonedRegulatingPointsIds,
                fetchTombstonedIdentifiableIds,
                fetchRegulatingPointIdsInVariant,
                fetchExternalAttributesInVariant,
                idExtractor
        );
    }

    public static Set<OwnerInfo> getExternalAttributesToTombstone(
            Map<Integer, List<String>> externalAttributesResourcesIdsByVariant,
            IntFunction<NetworkAttributes> fetchNetworkAttributes,
            TriFunction<Integer, Integer, List<String>, Set<OwnerInfo>> fetchExternalAttributesOwnerInfoInVariant,
            IntFunction<Set<String>> fetchTombstonedExternalAttributesIds,
            Set<OwnerInfo> externalAttributesToTombstoneFromEquipments
    ) {
        if (externalAttributesToTombstoneFromEquipments.isEmpty()) {
            return Set.of();
        }

        Set<OwnerInfo> externalAttributesResourcesInFullVariant = new HashSet<>();
        Set<String> tombstonedExternalAttributes = new HashSet<>();
        Set<Integer> fullVariant = new HashSet<>();
        // Retrieve external attributes from full variant
        for (Map.Entry<Integer, List<String>> entry : externalAttributesResourcesIdsByVariant.entrySet()) {
            int variantNum = entry.getKey();
            List<String> resourcesIds = entry.getValue();
            NetworkAttributes networkAttributes = fetchNetworkAttributes.apply(variantNum);
            int fullVariantNum = networkAttributes.getFullVariantNum();
            if (NetworkAttributes.isFullVariant(fullVariantNum)) {
                fullVariant.add(variantNum);
            }
            externalAttributesResourcesInFullVariant.addAll(fetchExternalAttributesOwnerInfoInVariant.apply(fullVariantNum, variantNum, resourcesIds));
            tombstonedExternalAttributes.addAll(fetchTombstonedExternalAttributesIds.apply(variantNum));
        }
        // Tombstone only external attributes existing in full variant and not already tombstoned
        return externalAttributesToTombstoneFromEquipments.stream().filter(owner ->
                externalAttributesResourcesInFullVariant.contains(owner) &&
                !tombstonedExternalAttributes.contains(owner.getEquipmentId()) &&
                !fullVariant.contains(owner.getVariantNum())).collect(Collectors.toSet());
    }

    public static <T> List<T> getIdentifiables(
            int variantNum,
            int fullVariantNum,
            Supplier<Set<String>> fetchTombstonedIdentifiableIds,
            IntFunction<List<T>> fetchIdentifiablesInVariant,
            Function<T, String> idExtractor,
            Supplier<List<String>> fetchIdentifiblesIdsInVariant) {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve identifiables directly
            return fetchIdentifiablesInVariant.apply(variantNum);
        }

        // Retrieve identifiables from the full variant first
        List<T> identifiables = fetchIdentifiablesInVariant.apply(fullVariantNum);

        // Retrieve identifiables in partial variant
        List<T> partialVariantIdentifiables = fetchIdentifiablesInVariant.apply(variantNum);

        // Retrieve ids in partial variant
        Set<String> partialVariantIds = fetchIdentifiblesIdsInVariant != null
                ? new HashSet<>(fetchIdentifiblesIdsInVariant.get())
                : partialVariantIdentifiables.stream()
                .map(idExtractor)
                .collect(Collectors.toSet());

        // Remove any resources in the partial variant or tombstoned
        Set<String> tombstonedIds = fetchTombstonedIdentifiableIds.get();
        identifiables.removeIf(resource ->
                partialVariantIds.contains(idExtractor.apply(resource)) || tombstonedIds.contains(idExtractor.apply(resource))
        );

        // Combine identifiables from full and partial variant
        identifiables.addAll(partialVariantIdentifiables);

        return identifiables;
    }

    public static <T extends Attributes> Optional<Resource<T>> getOptionalIdentifiable(
            String identifiableId,
            int variantNum,
            int fullVariantNum,
            Supplier<Set<String>> fetchTombstonedIdentifiableIds,
            IntFunction<Optional<Resource<T>>> fetchIdentifiableInVariant) {
        if (NetworkAttributes.isFullVariant(fullVariantNum)) {
            // If the variant is full, retrieve identifiables directly
            return fetchIdentifiableInVariant.apply(variantNum);
        }

        // Retrieve identifiable in partial variant
        Optional<Resource<T>> partialVariantIdentifiable = fetchIdentifiableInVariant.apply(variantNum);
        if (partialVariantIdentifiable.isPresent()) {
            return partialVariantIdentifiable;
        }

        Set<String> tombstonedIds = fetchTombstonedIdentifiableIds.get();
        if (tombstonedIds.contains(identifiableId)) {
            // Return empty if the identifiable is marked as tombstoned
            return Optional.empty();
        }

        // Retrieve identifiable from the full variant
        return fetchIdentifiableInVariant.apply(fullVariantNum);
    }
}
