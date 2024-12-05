/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.json;

import com.powsybl.iidm.network.LimitType;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
@ToString
@Builder
@AllArgsConstructor
@Getter
public class PermanentLimitSqlData {

    private String operationalLimitsGroupId;
    private double value;
    private Integer side;
    private LimitType limitType;

    public PermanentLimitSqlData() {
        // empty constructor for Jackson
    }

    public static PermanentLimitSqlData of(PermanentLimitAttributes permanentLimitAttributes) {
        return PermanentLimitSqlData.builder()
            .operationalLimitsGroupId(permanentLimitAttributes.getOperationalLimitsGroupId())
            .value(permanentLimitAttributes.getValue())
            .side(permanentLimitAttributes.getSide())
            .limitType(permanentLimitAttributes.getLimitType())
            .build();
    }

    public PermanentLimitAttributes toPermanentLimitAttributes() {
        return PermanentLimitAttributes.builder()
            .operationalLimitsGroupId(operationalLimitsGroupId)
            .value(value)
            .side(side)
            .limitType(limitType)
            .build();
    }
}
