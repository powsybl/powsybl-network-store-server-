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
            int srcVariantNum,
            Supplier<Set<String>> fetchTombstonedExternalAttributesIds,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<Map<T, U>> fetchExternalAttributesInVariant,
            Function<T, String> idExtractor) {
        if (srcVariantNum == -1) {
            // If the variant is full, retrieve external attributes directly
            return fetchExternalAttributesInVariant.apply(variantNum);
        }

        // Retrieve external attributes from the full variant first
        Map<T, U> externalAttributes = fetchExternalAttributesInVariant.apply(srcVariantNum);

        // Remove external attributes associated to tombstoned resources and tombstoned external attributes
        Set<String> tombstonedIds = fetchTombstonedIds.get();
        Set<String> tombstonedExternalAttributesIds = fetchTombstonedExternalAttributesIds.get();
        externalAttributes.keySet().removeIf(ownerInfo -> tombstonedIds.contains(idExtractor.apply(ownerInfo)) || tombstonedExternalAttributesIds.contains(idExtractor.apply(ownerInfo)));

        // Retrieve updated external attributes in partial variant
        Map<T, U> externalAttributesUpdatedInPartialVariant = fetchExternalAttributesInVariant.apply(variantNum);

        // Combine external attributes from full and partial variant
        externalAttributes.putAll(externalAttributesUpdatedInPartialVariant);

        return externalAttributes;
    }

    public static Set<OwnerInfo> getExternalAttributesToTombstone(
            Map<Integer, List<String>> externalAttributesResourcesIdsByVariant,
            IntFunction<Resource<NetworkAttributes>> fetchNetworkAttributes,
            TriFunction<Integer, Integer, List<String>, Set<OwnerInfo>> fetchExternalAttributesOwnerInfoInVariant,
            IntFunction<Set<String>> fetchTombstonedExternalAttributesIds,
            Set<OwnerInfo> externalAttributesToTombstoneFromEquipment
    ) {
        if (externalAttributesToTombstoneFromEquipment.isEmpty()) {
            return Set.of();
        }

        Set<OwnerInfo> externalAttributesResourcesInFullVariant = new HashSet<>();
        Set<String> tombstonedExternalAttributes = new HashSet<>();
        Set<Integer> fullVariant = new HashSet<>();
        // Retrieve external attributes from full variant
        for (Map.Entry<Integer, List<String>> entry : externalAttributesResourcesIdsByVariant.entrySet()) {
            int variantNum = entry.getKey();
            List<String> resourcesIds = entry.getValue();
            Resource<NetworkAttributes> networkAttributes = fetchNetworkAttributes.apply(variantNum);
            int srcVariantNum = networkAttributes.getAttributes().getSrcVariantNum();
            if (srcVariantNum == -1) {
                fullVariant.add(variantNum);
            }
            externalAttributesResourcesInFullVariant.addAll(fetchExternalAttributesOwnerInfoInVariant.apply(srcVariantNum, variantNum, resourcesIds));
            tombstonedExternalAttributes.addAll(fetchTombstonedExternalAttributesIds.apply(variantNum));
        }
        // Tombstone only external attributes existing in full variant and not already tombstoned
        return externalAttributesToTombstoneFromEquipment.stream().filter(owner ->
                externalAttributesResourcesInFullVariant.contains(owner) &&
                !tombstonedExternalAttributes.contains(owner.getEquipmentId()) &&
                !fullVariant.contains(owner.getVariantNum())).collect(Collectors.toSet());
    }

    public static <T> List<T> getIdentifiables(
            int variantNum,
            int srcVariantNum,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<List<T>> fetchIdentifiablesInVariant,
            Function<T, String> idExtractor,
            Supplier<List<String>> fetchUpdatedIdentifiblesIdsInVariant) {
        if (srcVariantNum == -1) {
            // If the variant is full, retrieve identifiables directly
            return fetchIdentifiablesInVariant.apply(variantNum);
        }

        // Retrieve identifiables from the full variant first
        List<T> identifiables = fetchIdentifiablesInVariant.apply(srcVariantNum);

        // Retrieve updated identifiables in partial variant
        List<T> updatedIdentifiables = fetchIdentifiablesInVariant.apply(variantNum);

        // Retrieve updated ids in partial variant
        Set<String> updatedIds = fetchUpdatedIdentifiblesIdsInVariant != null
                ? new HashSet<>(fetchUpdatedIdentifiblesIdsInVariant.get())
                : updatedIdentifiables.stream()
                .map(idExtractor)
                .collect(Collectors.toSet());

        // Remove any resources that have been updated in the current variant or tombstoned
        Set<String> tombstonedIds = fetchTombstonedIds.get();
        identifiables.removeIf(resource ->
                updatedIds.contains(idExtractor.apply(resource)) || tombstonedIds.contains(idExtractor.apply(resource))
        );

        // Combine identifiables from full and partial variant
        identifiables.addAll(updatedIdentifiables);

        return identifiables;
    }

    public static <T extends Attributes> Optional<Resource<T>> getOptionalIdentifiable(
            String identifiableId,
            int variantNum,
            int srcVariantNum,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<Optional<Resource<T>>> fetchIdentifiableInVariant) {
        if (srcVariantNum == -1) {
            // If the variant is full, retrieve identifiables directly
            return fetchIdentifiableInVariant.apply(variantNum);
        }

        // If the identifiable is tombstoned, return directly
        Set<String> tombstonedIds = fetchTombstonedIds.get();
        if (tombstonedIds.contains(identifiableId)) {
            return Optional.empty();
        }

        // Retrieve updated identifiable in partial variant
        Optional<Resource<T>> updatedIdentifiable = fetchIdentifiableInVariant.apply(variantNum);
        if (updatedIdentifiable.isPresent()) {
            return updatedIdentifiable;
        }

        // Retrieve identifiable from the full variant
        return fetchIdentifiableInVariant.apply(srcVariantNum);
    }
}
