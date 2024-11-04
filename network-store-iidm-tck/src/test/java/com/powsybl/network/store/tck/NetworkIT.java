/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.google.common.collect.Iterables;
import com.powsybl.iidm.network.*;
import com.powsybl.iidm.network.tck.AbstractNetworkTest;
import com.powsybl.iidm.network.test.NetworkTest1Factory;
import com.powsybl.iidm.network.util.Networks;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class NetworkIT extends AbstractNetworkTest {

    @Test
    @Override
    public void testWith() {
        // FIXME remove this when we fix the fact that we use the "Default" NetworkFactory in the base class
    }

    @Test
    @Override
    public void testScadaNetwork() {
        // FIXME test throws UnsupportedOperationException: Validation level below STEADY_STATE_HYPOTHESIS not supported
    }

    @Test
    @Override
    public void testStreams() {
        // FIXME remove this test when we use the release containing this PR : https://github.com/powsybl/powsybl-core/pull/3020
    }

    // see FIXME-Override below
    @Test
    @Override
    public void testNetwork1() {
        Network network = NetworkTest1Factory.create();
        assertSame(network, network.getNetwork());
        assertEquals(1, Iterables.size(network.getCountries()));
        assertEquals(1, network.getCountryCount());
        Country country1 = network.getCountries().iterator().next();
        assertEquals(1, Iterables.size(network.getSubstations()));
        assertEquals(1, Iterables.size(network.getSubstations(Country.FR, "TSO1", new String[]{"region1"})));
        assertEquals(1, network.getSubstationCount());
        assertEquals(2, network.getBusBreakerView().getBusCount());
        Substation substation1 = network.getSubstation("substation1");
        assertNotNull(substation1);
        assertEquals("substation1", substation1.getId());
        assertSame(country1, substation1.getCountry().orElse(null));
        assertEquals(1, substation1.getGeographicalTags().size());
        assertTrue(substation1.getGeographicalTags().contains("region1"));
        assertEquals(1, Iterables.size(network.getVoltageLevels()));
        assertEquals(1, network.getVoltageLevelCount());
        VoltageLevel voltageLevel1 = network.getVoltageLevel("voltageLevel1");
        assertNotNull(voltageLevel1);
        assertEquals("voltageLevel1", voltageLevel1.getId());
        assertEquals(400.0, voltageLevel1.getNominalV(), 0.0);
        assertSame(substation1, voltageLevel1.getSubstation().orElse(null));
        assertSame(TopologyKind.NODE_BREAKER, voltageLevel1.getTopologyKind());
        VoltageLevel.NodeBreakerView topology1 = voltageLevel1.getNodeBreakerView();
        assertEquals(0.0, topology1.getFictitiousP0(0), 0.0);
        assertEquals(0.0, topology1.getFictitiousQ0(0), 0.0);
        topology1.setFictitiousP0(0, 1.0).setFictitiousQ0(0, 2.0);
        assertEquals(1.0, topology1.getFictitiousP0(0), 0.0);
        assertEquals(2.0, topology1.getFictitiousQ0(0), 0.0);
        Map<String, Set<Integer>> nodesByBus = Networks.getNodesByBus(voltageLevel1);
        nodesByBus.forEach((busId, nodes) -> {
            if (nodes.contains(0)) {
                assertEquals(1.0, voltageLevel1.getBusView().getBus(busId).getFictitiousP0(), 0.0);
            } else if (nodes.contains(1)) {
                assertEquals(2.0, voltageLevel1.getBusView().getBus(busId).getFictitiousP0(), 0.0);
            }
        });
        assertEquals(6, topology1.getMaximumNodeIndex());
        assertEquals(2, Iterables.size(topology1.getBusbarSections()));
        assertEquals(2, topology1.getBusbarSectionCount());
        assertEquals(2, Iterables.size(network.getBusbarSections()));
        assertEquals(2, network.getBusbarSectionCount());
        assertEquals(2L, network.getBusbarSectionStream().count());
        BusbarSection voltageLevel1BusbarSection1 = topology1.getBusbarSection("voltageLevel1BusbarSection1");
        assertNotNull(voltageLevel1BusbarSection1);
        assertEquals("voltageLevel1BusbarSection1", voltageLevel1BusbarSection1.getId());
        BusbarSection voltageLevel1BusbarSection2 = topology1.getBusbarSection("voltageLevel1BusbarSection2");
        assertNotNull(voltageLevel1BusbarSection2);
        assertEquals("voltageLevel1BusbarSection2", voltageLevel1BusbarSection2.getId());
        assertEquals(5, Iterables.size(topology1.getSwitches()));
        assertEquals(5, topology1.getSwitchCount());
        VoltageLevel voltageLevel2 = (substation1.newVoltageLevel().setId("VL2")).setNominalV(320.0).setTopologyKind(TopologyKind.NODE_BREAKER).add();
        assertNull(voltageLevel2.getNodeBreakerView().getBusbarSection("voltageLevel1BusbarSection1"));
        assertEquals(Arrays.asList(network.getSwitch("generator1Disconnector1"), network.getSwitch("generator1Breaker1")), topology1.getSwitches(6));
        assertEquals(Arrays.asList(network.getSwitch("load1Disconnector1"), network.getSwitch("load1Breaker1")), topology1.getSwitchStream(3).collect(Collectors.toList()));
        assertEquals(Collections.singletonList(network.getSwitch("load1Disconnector1")), topology1.getSwitches(2));
        assertEquals(5, Iterables.size(network.getSwitches()));
        assertEquals(5, network.getSwitchCount());
        assertEquals(5L, network.getSwitchStream().count());
        Switch voltageLevel1Breaker1 = topology1.getSwitch("voltageLevel1Breaker1");
        assertNotNull(voltageLevel1Breaker1);
        assertEquals("voltageLevel1Breaker1", voltageLevel1Breaker1.getId());
        assertFalse(voltageLevel1Breaker1.isOpen());
        assertTrue(voltageLevel1Breaker1.isRetained());
        assertSame(SwitchKind.BREAKER, voltageLevel1Breaker1.getKind());
        assertSame(voltageLevel1BusbarSection1.getTerminal().getNodeBreakerView().getNode(), topology1.getNode1(voltageLevel1Breaker1.getId()));
        assertSame(voltageLevel1BusbarSection2.getTerminal().getNodeBreakerView().getNode(), topology1.getNode2(voltageLevel1Breaker1.getId()));
        assertEquals(1, Iterables.size(voltageLevel1.getLoads()));
        assertEquals(1, voltageLevel1.getLoadCount());
        Load load1 = network.getLoad("load1");
        assertNotNull(load1);
        assertEquals("load1", load1.getId());
        assertEquals(2, load1.getTerminal().getNodeBreakerView().getNode());
        assertEquals(10.0, load1.getP0(), 0.0);
        assertEquals(3.0, load1.getQ0(), 0.0);
        Generator generator1 = network.getGenerator("generator1");
        assertNotNull(generator1);
        assertEquals("generator1", generator1.getId());
        assertEquals(5, generator1.getTerminal().getNodeBreakerView().getNode());
        assertEquals(200.0, generator1.getMinP(), 0.0);
        assertEquals(900.0, generator1.getMaxP(), 0.0);
        assertSame(EnergySource.NUCLEAR, generator1.getEnergySource());
        assertTrue(generator1.isVoltageRegulatorOn());
        assertEquals(900.0, generator1.getTargetP(), 0.0);
        assertEquals(380.0, generator1.getTargetV(), 0.0);
        ReactiveCapabilityCurve rcc1 = generator1.getReactiveLimits(ReactiveCapabilityCurve.class);
        assertEquals(2, rcc1.getPointCount());
        assertEquals(500.0, rcc1.getMaxQ(500.0), 0.0);
        assertEquals(300.0, rcc1.getMinQ(500.0), 0.0);
        assertEquals(2, Iterables.size(voltageLevel1.getBusBreakerView().getBuses()));
        assertEquals(2, voltageLevel1.getBusBreakerView().getBusCount());
        Bus busCalc1 = voltageLevel1BusbarSection1.getTerminal().getBusBreakerView().getBus();
        Bus busCalc2 = voltageLevel1BusbarSection2.getTerminal().getBusBreakerView().getBus();
        // FIXME-Override use asserEquals rather than assertSame on CalculatedBus (id vs ptr equality)
        assertEquals(busCalc1, load1.getTerminal().getBusBreakerView().getBus());
        assertEquals(busCalc2, generator1.getTerminal().getBusBreakerView().getBus());
        assertEquals(0, busCalc1.getConnectedComponent().getNum());
        assertEquals(0, busCalc2.getConnectedComponent().getNum());
        assertEquals(1, Iterables.size(voltageLevel1.getBusView().getBuses()));
        Bus busCalc = voltageLevel1BusbarSection1.getTerminal().getBusView().getBus();
        assertEquals(busCalc, voltageLevel1BusbarSection2.getTerminal().getBusView().getBus());
        assertEquals(busCalc, load1.getTerminal().getBusView().getBus());
        assertEquals(busCalc, generator1.getTerminal().getBusView().getBus());
        // FIXME-Override KO Assertions.assertEquals(0, busCalc.getConnectedComponent().getNum());

        // FIXME-Override properties assertions ko busCalc, cause CalculatedBus is NOT AbstractIdentifiable in network-store
        /*NetworkListener exceptionListener = (NetworkListener) Mockito.mock(DefaultNetworkListener.class);
        ((NetworkListener)Mockito.doThrow(new Throwable[]{new UnsupportedOperationException()}).when(exceptionListener)).onElementAdded((Identifiable)Mockito.any(), Mockito.anyString(), Mockito.any());
        ((NetworkListener)Mockito.doThrow(new Throwable[]{new UnsupportedOperationException()}).when(exceptionListener)).onElementReplaced((Identifiable)Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any());
        NetworkListener mockedListener = (NetworkListener)Mockito.mock(DefaultNetworkListener.class);
        String key = "keyTest";
        String value = "ValueTest";
        Assertions.assertFalse(busCalc.hasProperty());
        Assertions.assertTrue(busCalc.getPropertyNames().isEmpty());
        busCalc.setProperty("listeners", "no listeners");
        busCalc.setProperty("listeners", "no listeners");
        Mockito.verifyNoMoreInteractions(new Object[]{mockedListener});
        Mockito.verifyNoMoreInteractions(new Object[]{exceptionListener});
        network.addListener(mockedListener);
        network.addListener(exceptionListener);
        busCalc.setProperty(key, value);
        Assertions.assertTrue(busCalc.hasProperty());
        Assertions.assertTrue(busCalc.hasProperty(key));
        Assertions.assertEquals(value, busCalc.getProperty(key));
        Assertions.assertEquals("default", busCalc.getProperty("invalid", "default"));
        Assertions.assertEquals(2, busCalc.getPropertyNames().size());
        ((NetworkListener)Mockito.verify(mockedListener, Mockito.times(1))).onElementAdded(busCalc, "properties[" + key + "]", value);
        String value2 = "ValueTest2";
        busCalc.setProperty(key, value2);
        ((NetworkListener)Mockito.verify(mockedListener, Mockito.times(1))).onElementReplaced(busCalc, "properties[" + key + "]", value, value2);
        busCalc.setProperty(key, value2);
        Mockito.verifyNoMoreInteractions(new Object[]{mockedListener});
        network.removeListener(mockedListener);
        busCalc.setProperty(key, value);
        Mockito.verifyNoMoreInteractions(new Object[]{mockedListener});*/

        // FIXME-Override cannot use EQUIPMENT level (cf error 'Validation level below STEADY_STATE_HYPOTHESIS not supported')
        /*Assertions.assertEquals(ValidationLevel.STEADY_STATE_HYPOTHESIS, network.getValidationLevel());
        network.runValidationChecks();
        network.setMinimumAcceptableValidationLevel(ValidationLevel.EQUIPMENT);
        Assertions.assertEquals(ValidationLevel.STEADY_STATE_HYPOTHESIS, network.getValidationLevel());
        network.getLoad("load1").setP0(0.0);
        ((LoadAdder)((LoadAdder)voltageLevel1.newLoad().setId("unchecked")).setP0(1.0).setQ0(1.0).setNode(3)).add();
        Assertions.assertEquals(ValidationLevel.STEADY_STATE_HYPOTHESIS, network.getValidationLevel());
        network.setMinimumAcceptableValidationLevel(ValidationLevel.EQUIPMENT);
        Load unchecked2 = ((LoadAdder)((LoadAdder)voltageLevel1.newLoad().setId("unchecked2")).setNode(10)).add();
        Assertions.assertEquals(ValidationLevel.EQUIPMENT, network.getValidationLevel());
        unchecked2.setP0(0.0).setQ0(0.0);
        Assertions.assertEquals(ValidationLevel.STEADY_STATE_HYPOTHESIS, network.getValidationLevel());
        network.setMinimumAcceptableValidationLevel(ValidationLevel.STEADY_STATE_HYPOTHESIS);
        */
    }

    @Test
    @Override
    public void testPermanentLimitViaAdder() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }

    @Test
    @Override
    public void testPermanentLimitOnUnselectedOperationalLimitsGroup() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }

    @Test
    @Override
    public void testPermanentLimitOnSelectedOperationalLimitsGroup() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }
}
