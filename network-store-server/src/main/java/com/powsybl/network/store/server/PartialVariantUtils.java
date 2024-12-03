/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.network.store.model.NetworkAttributes;
import com.powsybl.network.store.model.Resource;
import com.powsybl.network.store.server.dto.OwnerInfo;
import org.apache.commons.lang3.function.TriFunction;

import java.util.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public final class PartialVariantUtils {
    private PartialVariantUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class can not be initialize.");
    }

    public static <T> Map<OwnerInfo, T> getExternalAttributes(
            UUID networkUuid,
            int variantNum,
            Resource<NetworkAttributes> network,
            List<String> tombstonedIds,
            TriFunction<UUID, Integer, Integer, Map<OwnerInfo, T>> fetchFunction
    ) {
        int srcVariantNum = network.getAttributes().getSrcVariantNum();

        if (srcVariantNum == -1) {
            // If the variant is full, retrieve external attributes directly
            return fetchFunction.apply(networkUuid, variantNum, variantNum);
        }

        // Retrieve external attributes from the full variant first
        Map<OwnerInfo, T> externalAttributes = fetchFunction.apply(networkUuid, srcVariantNum, variantNum);

        // Retrieve updated external attributes in partial variant
        Map<OwnerInfo, T> externalAttributesUpdatedInPartialVariant = fetchFunction.apply(networkUuid, variantNum, variantNum);

        // Combine base and updated externalAttributes
        externalAttributes.putAll(externalAttributesUpdatedInPartialVariant);

        // Remove tombstoned resources in the current variant
        externalAttributes.keySet().removeIf(ownerInfo -> tombstonedIds.contains(ownerInfo.getEquipmentId()));
        return externalAttributes;
    }
}
