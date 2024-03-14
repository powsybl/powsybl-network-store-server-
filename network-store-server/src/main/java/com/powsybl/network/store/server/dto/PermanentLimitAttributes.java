/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.powsybl.iidm.network.LimitType;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Ayoub LABIDI <ayoub.labidi at rte-france.com>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Schema(description = "Permanent limit attributes")
public class PermanentLimitAttributes {

    @JsonIgnore
    @Schema(description = "Operational limits group Id", required = true)
    private String operationalLimitsGroupId;

    @JsonIgnore
    @Schema(description = "Permanent limit side", required = true)
    private Integer side;

    @JsonIgnore
    @Schema(description = "Permanent limit type", required = true)
    private LimitType limitType;

    @Schema(description = "Permanent limit value")
    private double value;

}
