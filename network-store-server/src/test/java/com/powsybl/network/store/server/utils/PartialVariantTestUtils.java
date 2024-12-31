/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.utils;

import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.NetworkStoreRepository;

import java.util.List;
import java.util.UUID;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
public final class PartialVariantTestUtils {
    private PartialVariantTestUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class can not be initialize.");
    }

    public static Resource<LineAttributes> createLine(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String lineId, String voltageLevel1, String voltageLevel2) {
        Resource<LineAttributes> line = Resource.lineBuilder()
                .id(lineId)
                .variantNum(variantNum)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1(voltageLevel1)
                        .voltageLevelId2(voltageLevel2)
                        .build())
                .build();
        repository.createLines(networkUuid, List.of(line));
        return line;
    }

    public static void createLineAndLoad(NetworkStoreRepository repository, UUID networkUuid, int variantNum, String loadId, String lineId, String voltageLevel1, String voltageLevel2) {
        createLine(repository, networkUuid, variantNum, lineId, voltageLevel1, voltageLevel2);
        Resource<LoadAttributes> load1 = buildLoad(loadId, variantNum, voltageLevel1);
        repository.createLoads(networkUuid, List.of(load1));
    }

    public static Resource<LoadAttributes> buildLoad(String loadId, int variantNum, String voltageLevel) {
        return Resource.loadBuilder()
                .id(loadId)
                .variantNum(variantNum)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId(voltageLevel)
                        .build())
                .build();
    }

    public static void createNetwork(NetworkStoreRepository repository, UUID networkUuid, String networkId, int variantNum, String variantId, CloneStrategy cloneStrategy, int fullVariantNum) {
        Resource<NetworkAttributes> network1 = Resource.networkBuilder()
                .id(networkId)
                .variantNum(variantNum)
                .attributes(NetworkAttributes.builder()
                        .uuid(networkUuid)
                        .variantId(variantId)
                        .cloneStrategy(cloneStrategy)
                        .fullVariantNum(fullVariantNum)
                        .build())
                .build();
        repository.createNetworks(List.of(network1));
    }

    public static void createFullVariantNetwork(NetworkStoreRepository repository, UUID networkUuid, String networkId, int variantNum, String variantId, CloneStrategy cloneStrategy) {
        createNetwork(repository, networkUuid, networkId, variantNum, variantId, cloneStrategy, -1);
    }
}
