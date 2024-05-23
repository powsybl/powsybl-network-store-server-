/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.integration;

import com.powsybl.commons.datasource.ResourceDataSource;
import com.powsybl.commons.datasource.ResourceSet;
import com.powsybl.iidm.network.*;
import com.powsybl.iidm.network.extensions.BusbarSectionPosition;
import com.powsybl.iidm.network.extensions.BusbarSectionPositionAdder;
import com.powsybl.iidm.network.extensions.ConnectablePosition;
import com.powsybl.iidm.network.extensions.ConnectablePositionAdder;
import com.powsybl.network.store.client.NetworkStoreService;
import com.powsybl.network.store.client.PreloadingStrategy;
import com.powsybl.network.store.client.RestClient;
import com.powsybl.network.store.client.RestClientImpl;
import org.apache.commons.io.FilenameUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
public final class TestUtils {

    private TestUtils() {
    }

    static NetworkStoreService createNetworkStoreService(int randomServerPort) {
        return createNetworkStoreService(null, randomServerPort);
    }

    static NetworkStoreService createNetworkStoreService(RestClientMetrics metrics, int randomServerPort) {
        RestClient restClient = new RestClientImpl(getBaseUrl(randomServerPort));
        if (metrics != null) {
            restClient = new TestRestClient(restClient, metrics);
        }
        return new NetworkStoreService(restClient, PreloadingStrategy.NONE);
    }

    private static String getBaseUrl(int randomServerPort) {
        return "http://localhost:" + randomServerPort + "/";
    }

    static ResourceDataSource getResource(String fileName, String path) {
        return new ResourceDataSource(FilenameUtils.getBaseName(fileName),
                new ResourceSet(FilenameUtils.getPath(path),
                        FilenameUtils.getName(fileName)));
    }

    static Substation createSubstation(Network n, String id, String name, Country country) {
        return n.newSubstation()
                .setId(id)
                .setName(name)
                .setCountry(country)
                .add();
    }

    static VoltageLevel createVoltageLevel(Substation s, String id, String name,
                                                   TopologyKind topology, double vNom, int nodeCount) {
        VoltageLevel vl = s.newVoltageLevel()
                .setId(id)
                .setName(name)
                .setTopologyKind(topology)
                .setNominalV(vNom)
                .add();
        return vl;
    }

    static void createBusBarSection(VoltageLevel vl, String id, String name, int node, int busbarIndex, int sectionIndex) {
        BusbarSection bbs = vl.getNodeBreakerView().newBusbarSection()
                .setId(id)
                .setName(name)
                .setNode(node)
                .add();
        bbs.newExtension(BusbarSectionPositionAdder.class).withBusbarIndex(busbarIndex).withSectionIndex(sectionIndex).add();
        BusbarSectionPosition bbsp = bbs.getExtension(BusbarSectionPosition.class);
        assertNotNull(bbsp);
        assertEquals(busbarIndex, bbsp.getBusbarIndex());
        assertEquals(sectionIndex, bbsp.getSectionIndex());
        bbsp = bbs.getExtensionByName("position");
        assertNotNull(bbsp);
        assertEquals(busbarIndex, bbsp.getBusbarIndex());
        assertEquals(sectionIndex, bbsp.getSectionIndex());
    }

    static void createSwitch(VoltageLevel vl, String id, String name, SwitchKind kind, boolean retained, boolean open, boolean fictitious, int node1, int node2) {
        vl.getNodeBreakerView().newSwitch()
                .setId(id)
                .setName(name)
                .setKind(kind)
                .setRetained(retained)
                .setOpen(open)
                .setFictitious(fictitious)
                .setNode1(node1)
                .setNode2(node2)
                .add();
    }

    static void createLoad(VoltageLevel vl, String id, String name, String feederName, int feederOrder,
                                   ConnectablePosition.Direction direction, int node, double p0, double q0) {
        Load load = vl.newLoad()
                .setId(id)
                .setName(name)
                .setNode(node)
                .setP0(p0)
                .setQ0(q0)
                .add();
        load.newExtension(ConnectablePositionAdder.class).newFeeder()
                .withName(feederName).withOrder(feederOrder).withDirection(direction).add().add();
        ConnectablePosition cp = load.getExtension(ConnectablePosition.class);
        assertNotNull(cp);
        assertEquals(feederName, cp.getFeeder().getName().orElseThrow());
        assertEquals(feederOrder, cp.getFeeder().getOrder().orElseThrow().intValue());
        assertEquals(direction, cp.getFeeder().getDirection());
        cp = load.getExtensionByName("position");
        assertNotNull(cp);
        assertEquals(feederName, cp.getFeeder().getName().orElseThrow());
        assertEquals(feederOrder, cp.getFeeder().getOrder().orElseThrow().intValue());
        assertEquals(direction, cp.getFeeder().getDirection());
    }
}
