/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.json;

import com.powsybl.iidm.network.LimitType;
import com.powsybl.network.store.model.TemporaryLimitAttributes;
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
public class TemporaryLimitSqlData {
    private String operationalLimitsGroupId;
    private Integer side;
    private LimitType limitType;
    private String name;
    private double value;
    private Integer acceptableDuration;
    private boolean fictitious;

    public TemporaryLimitSqlData() {
        // empty constructor for Jackson
    }

    public static TemporaryLimitSqlData of(TemporaryLimitAttributes temporaryLimit) {
        return TemporaryLimitSqlData.builder()
            .operationalLimitsGroupId(temporaryLimit.getOperationalLimitsGroupId())
            .side(temporaryLimit.getSide())
            .limitType(temporaryLimit.getLimitType())
            .name(temporaryLimit.getName())
            .value(temporaryLimit.getValue())
            .acceptableDuration(temporaryLimit.getAcceptableDuration())
            .fictitious(temporaryLimit.isFictitious())
            .build();
    }

    public TemporaryLimitAttributes toTemporaryLimitAttributes() {
        return TemporaryLimitAttributes.builder()
            .operationalLimitsGroupId(operationalLimitsGroupId)
            .side(side)
            .limitType(limitType)
            .name(name)
            .value(value)
            .acceptableDuration(acceptableDuration)
            .fictitious(fictitious)
            .build();
    }
}
