/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.dto;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.powsybl.network.store.model.TemporaryLimitAttributes;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author Ayoub LABIDI <ayoub.labidi at rte-france.com>
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "limits attributes")
public class LimitsInfos {

    @Schema(description = "List of permanent limits")
    private List<PermanentLimitAttributes> permanentLimits = new ArrayList<>();

    @Schema(description = "List of temporary limits")
    private List<TemporaryLimitAttributes> temporaryLimits = new ArrayList<>();

}
