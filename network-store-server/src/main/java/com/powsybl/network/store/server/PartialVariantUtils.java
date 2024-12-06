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
            Resource<NetworkAttributes> network,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<Map<T, U>> fetchExternalAttributesForVariant,
            Function<T, String> idExtractor) {
        int srcVariantNum = network.getAttributes().getSrcVariantNum();

        if (srcVariantNum == -1) {
            // If the variant is full, retrieve external attributes directly
            return fetchExternalAttributesForVariant.apply(variantNum);
        }

        // Retrieve external attributes from the full variant first
        Map<T, U> externalAttributes = fetchExternalAttributesForVariant.apply(srcVariantNum);

        // Retrieve updated external attributes in partial variant
        Map<T, U> externalAttributesUpdatedInPartialVariant = fetchExternalAttributesForVariant.apply(variantNum);

        // Combine external attributes from full and partial variant
        externalAttributes.putAll(externalAttributesUpdatedInPartialVariant);

        // Remove external attributes associated to tombstoned resources
        Set<String> tombstonedIds = fetchTombstonedIds.get();
        externalAttributes.keySet().removeIf(ownerInfo -> tombstonedIds.contains(idExtractor.apply(ownerInfo)));
        return externalAttributes;
    }

    public static <T> List<T> getIdentifiables(
            int variantNum,
            Resource<NetworkAttributes> network,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<List<T>> fetchFunction,
            Function<T, String> idExtractor,
            Supplier<List<String>> fetchIdsFunction) {
        int srcVariantNum = network.getAttributes().getSrcVariantNum();

        if (srcVariantNum == -1) {
            // If the variant is full, retrieve identifiables directly
            return fetchFunction.apply(variantNum);
        }

        // Retrieve identifiables from the full variant first
        List<T> identifiables = fetchFunction.apply(srcVariantNum);

        // Retrieve updated identifiables in partial variant
        List<T> updatedIdentifiables = fetchFunction.apply(variantNum);

        // Retrieve updated ids in partial variant
        Set<String> updatedIds = fetchIdsFunction != null
                ? new HashSet<>(fetchIdsFunction.get())
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
            Resource<NetworkAttributes> network,
            Supplier<Set<String>> fetchTombstonedIds,
            IntFunction<Optional<Resource<T>>> fetchFunction) {
        int srcVariantNum = network.getAttributes().getSrcVariantNum();

        if (srcVariantNum == -1) {
            // If the variant is full, retrieve identifiables directly
            return fetchFunction.apply(variantNum);
        }

        // If the identifiable is tombstoned, return directly
        Set<String> tombstonedIds = fetchTombstonedIds.get();
        if (tombstonedIds.contains(identifiableId)) {
            return Optional.empty();
        }

        // Retrieve updated identifiable in partial variant
        Optional<Resource<T>> updatedIdentifiable = fetchFunction.apply(variantNum);
        if (updatedIdentifiable.isPresent()) {
            return updatedIdentifiable;
        }

        // Retrieve identifiable from the full variant
        return fetchFunction.apply(srcVariantNum);
    }
}
