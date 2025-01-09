/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.dto;

import com.powsybl.network.store.model.RegulatingTapChangerType;
import com.powsybl.network.store.model.ResourceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RegulatingOwnerInfo {

    public RegulatingOwnerInfo(String equipmentId, ResourceType equipmentType, UUID networkUuid, int variantNum) {
        this(equipmentId, equipmentType, RegulatingTapChangerType.NONE, networkUuid, variantNum);
    }

    private String equipmentId;

    private ResourceType equipmentType;

    private RegulatingTapChangerType regulatingTapChangerType;

    private UUID networkUuid;

    private int variantNum;
}
