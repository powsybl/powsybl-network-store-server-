/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.client;

import com.powsybl.iidm.network.ConnectableType;
import com.powsybl.network.store.model.*;

import java.util.Map;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class BusBreakerTopology extends AbstractTopology<String> {

    public static final BusBreakerTopology INSTANCE = new BusBreakerTopology();

    @Override
    protected String getNodeOrBus(Vertex vertex) {
        return vertex.getBus();
    }

    @Override
    protected Vertex createVertex(String id, ConnectableType connectableType, String nodeOrBus, String side) {
        return new Vertex(id, connectableType, null, nodeOrBus, side);
    }

    @Override
    public <U extends InjectionAttributes> String getInjectionNodeOrBus(Resource<U> resource) {
        return resource.getAttributes().getBus();
    }

    @Override
    protected <U extends BranchAttributes> String getBranchNodeOrBus1(Resource<U> resource) {
        return resource.getAttributes().getBus1();
    }

    @Override
    protected <U extends BranchAttributes> String getBranchNodeOrBus2(Resource<U> resource) {
        return resource.getAttributes().getBus2();
    }

    @Override
    protected <U extends ThreeWindingsTransformerAttributes> String get3wtNodeOrBus1(Resource<U> resource) {
        return resource.getAttributes().getLeg1().getBus();
    }

    @Override
    protected <U extends ThreeWindingsTransformerAttributes> String get3wtNodeOrBus2(Resource<U> resource) {
        return resource.getAttributes().getLeg2().getBus();
    }

    @Override
    protected <U extends ThreeWindingsTransformerAttributes> String get3wtNodeOrBus3(Resource<U> resource) {
        return resource.getAttributes().getLeg3().getBus();
    }

    @Override
    protected <U extends SwitchAttributes> String getSwitchNodeOrBus1(Resource<U> resource) {
        return resource.getAttributes().getBus1();
    }

    @Override
    protected <U extends SwitchAttributes> String getSwitchNodeOrBus2(Resource<U> resource) {
        return resource.getAttributes().getBus2();
    }

    @Override
    protected void setNodeOrBusToCalculatedBusNum(Resource<VoltageLevelAttributes> voltageLevelResource, Map<String, Integer> nodeOrBusToCalculatedBusNum) {
        voltageLevelResource.getAttributes().setBusToCalculatedBus(nodeOrBusToCalculatedBusNum);
    }

    @Override
    protected Map<String, Integer> getNodeOrBusToCalculatedBusNum(Resource<VoltageLevelAttributes> voltageLevelResource) {
        return voltageLevelResource.getAttributes().getBusToCalculatedBus();
    }

    @Override
    protected CalculatedBus createCalculatedBus(NetworkObjectIndex index, Resource<VoltageLevelAttributes> voltageLevelResource,
                                                int calculatedBusNum) {
        CalculatedBusAttributes calculatedBusAttributes = voltageLevelResource.getAttributes().getCalculatedBuses().get(calculatedBusNum);
        String firstBus = calculatedBusAttributes.getVertices().stream().map(Vertex::getBus).min(String::compareTo).orElseThrow(IllegalStateException::new);
        Resource<ConfiguredBusAttributes> firstBusResource = index.getStoreClient().getConfiguredBus(index.getNetwork().getUuid(), firstBus)
                .orElseThrow(IllegalStateException::new);
        String busId = firstBus + "_merge";
        String busName = null;
        if (firstBusResource.getAttributes().getName() != null) {
            busName = firstBusResource.getAttributes().getName() + "_merge";
        }
        return new CalculatedBus(index, voltageLevelResource.getId(), busId, busName, voltageLevelResource, calculatedBusNum);
    }
}
