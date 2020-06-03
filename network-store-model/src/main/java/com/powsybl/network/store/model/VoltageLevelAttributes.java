/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.powsybl.iidm.network.TopologyKind;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Data
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ApiModel("Voltage level attributes")
public class VoltageLevelAttributes extends AbstractAttributes implements IdentifiableAttributes {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Substation ID")
    private String substationId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Voltage level name")
    private String name;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("fictitious")
    private boolean fictitious;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Properties")
    private Map<String, String> properties;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Nominal voltage in kV")
    private double nominalV;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Low voltage limit in kV")
    private double lowVoltageLimit;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("High voltage limit in kV")
    private double highVoltageLimit;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Topology kind")
    private TopologyKind topologyKind;

    @ApiModelProperty("Internal connection of the voltage level")
    @Builder.Default
    private List<InternalConnectionAttributes> internalConnections = new ArrayList<>();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Calculated buses")
    private List<CalculatedBusAttributes> calculatedBuses;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Node to calculated bus")
    private Map<Integer, Integer> nodeToCalculatedBus;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ApiModelProperty("Bus to calculated bus")
    private Map<String, Integer> busToCalculatedBus;

    public VoltageLevelAttributes(VoltageLevelAttributes other) {
        super(other);
        this.substationId = other.substationId;
        this.name = other.name;
        this.fictitious = other.fictitious;
        this.properties = other.properties;
        this.nominalV = other.nominalV;
        this.lowVoltageLimit = other.lowVoltageLimit;
        this.highVoltageLimit = other.highVoltageLimit;
        this.topologyKind = other.topologyKind;
        this.internalConnections = other.internalConnections;
        this.calculatedBuses = other.calculatedBuses;
        this.nodeToCalculatedBus = other.nodeToCalculatedBus;
        this.busToCalculatedBus = other.busToCalculatedBus;
    }
}
