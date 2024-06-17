/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.integration;

import com.powsybl.cgmes.conformity.CgmesConformity1Catalog;
import com.powsybl.cgmes.conformity.CgmesConformity1ModifiedCatalog;
import com.powsybl.cgmes.conversion.CgmesImport;
import com.powsybl.cgmes.extensions.*;
import com.powsybl.cgmes.model.CgmesMetadataModel;
import com.powsybl.cgmes.model.CgmesSubset;
import com.powsybl.commons.PowsyblException;
import com.powsybl.commons.datasource.ReadOnlyDataSource;
import com.powsybl.commons.extensions.Extension;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.*;
import com.powsybl.iidm.network.extensions.*;
import com.powsybl.iidm.network.test.BatteryNetworkFactory;
import com.powsybl.iidm.network.test.EurostagTutorialExample1Factory;
import com.powsybl.iidm.network.test.HvdcTestNetwork;
import com.powsybl.iidm.network.test.SvcTestCaseFactory;
import com.powsybl.iidm.network.test.ThreeWindingsTransformerNetworkFactory;
import com.powsybl.network.store.client.NetworkStoreService;
import com.powsybl.network.store.iidm.impl.NetworkImpl;
import com.powsybl.network.store.model.BaseVoltageSourceAttribute;
import com.powsybl.network.store.model.CgmesMetadataModelAttributes;
import com.powsybl.network.store.model.CgmesMetadataModelsAttributes;
import com.powsybl.network.store.model.CimCharacteristicsAttributes;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

import static com.powsybl.network.store.integration.TestUtils.*;
import static org.junit.Assert.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextHierarchy({
    @ContextConfiguration(classes = {NetworkStoreApplication.class, NetworkStoreService.class})
})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class NetworkStoreExtensionsIT {

    @DynamicPropertySource
    static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
        UUID uuid = UUID.randomUUID();
        registry.add("testDbSuffix", () -> uuid);
    }

    @LocalServerPort
    private int randomServerPort;

    @Test
    public void testActivePowerControlExtension() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Generator gen = network.getGenerator("GEN");
            gen.newExtension(ActivePowerControlAdder.class)
                    .withParticipate(true)
                    .withDroop(6.3f)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            Generator gen = network.getGenerator("GEN");
            ActivePowerControl<Generator> activePowerControl = gen.getExtension(ActivePowerControl.class);
            assertNotNull(activePowerControl);
            assertTrue(activePowerControl.isParticipate());
            assertEquals(6.3f, activePowerControl.getDroop(), 0f);
            assertNotNull(gen.getExtensionByName("activePowerControl"));
        }
    }

    @Test
    public void testGeneratorStartup() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Generator gen = network.getGenerator("GEN");
            gen.newExtension(GeneratorStartupAdder.class)
                    .withPlannedActivePowerSetpoint(1.0)
                    .withStartupCost(2.0)
                    .withMarginalCost(3.0)
                    .withPlannedOutageRate(4.0)
                    .withForcedOutageRate(5.0)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            Generator gen = network.getGenerator("GEN");
            GeneratorStartup generatorStartup = gen.getExtension(GeneratorStartup.class);
            assertNotNull(generatorStartup);
            assertEquals(1.0f, generatorStartup.getPlannedActivePowerSetpoint(), 0f);
            assertEquals(2.0f, generatorStartup.getStartupCost(), 0f);
            assertEquals(3.0f, generatorStartup.getMarginalCost(), 0f);
            assertEquals(4.0f, generatorStartup.getPlannedOutageRate(), 0f);
            assertEquals(5.0f, generatorStartup.getForcedOutageRate(), 0f);
            assertNotNull(gen.getExtensionByName(GeneratorStartup.NAME));
        }
    }

    @Test
    public void testGeneratorShortCircuit() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Generator gen = network.getGenerator("GEN");
            assertNull(gen.getExtension(GeneratorShortCircuit.class));
            assertNull(gen.getExtensionByName(GeneratorShortCircuit.NAME));
            assertTrue(gen.getExtensions().isEmpty());
            GeneratorShortCircuitAdder circuitAdder = gen.newExtension(GeneratorShortCircuitAdder.class).withDirectTransX(Double.NaN);
            assertThrows(PowsyblException.class, () -> circuitAdder.add());
            circuitAdder.withDirectSubtransX(20.)
                    .withDirectTransX(30.)
                    .withStepUpTransformerX(50.)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            Generator gen = network.getGenerator("GEN");
            GeneratorShortCircuit generatorShortCircuit = gen.getExtension(GeneratorShortCircuit.class);
            assertNotNull(generatorShortCircuit);
            assertEquals(20., generatorShortCircuit.getDirectSubtransX(), 0);
            assertEquals(30., generatorShortCircuit.getDirectTransX(), 0);
            assertEquals(50., generatorShortCircuit.getStepUpTransformerX(), 0);
            assertNotNull(gen.getExtensionByName(GeneratorShortCircuit.NAME));
            assertEquals(GeneratorShortCircuit.NAME, generatorShortCircuit.getName());

            assertThrows(PowsyblException.class, () -> generatorShortCircuit.setDirectTransX(Double.NaN));
            generatorShortCircuit.setDirectSubtransX(23.);
            generatorShortCircuit.setDirectTransX(32.);
            generatorShortCircuit.setStepUpTransformerX(44.);
            assertEquals(23., generatorShortCircuit.getDirectSubtransX(), 0);
            assertEquals(32., generatorShortCircuit.getDirectTransX(), 0);
            assertEquals(44., generatorShortCircuit.getStepUpTransformerX(), 0);
        }
    }

    @Test
    public void testIdentifiableShortCircuit() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            VoltageLevel vl = network.getVoltageLevel("VLGEN");
            assertNull(vl.getExtension(IdentifiableShortCircuit.class));
            assertNull(vl.getExtensionByName(IdentifiableShortCircuit.NAME));
            assertTrue(vl.getExtensions().isEmpty());
            IdentifiableShortCircuitAdder shortCircuitAdder = vl.newExtension(IdentifiableShortCircuitAdder.class);
            shortCircuitAdder.withIpMin(80.)
                    .withIpMax(220.)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            VoltageLevel vl = network.getVoltageLevel("VLGEN");
            IdentifiableShortCircuit identifiableShortCircuit = vl.getExtension(IdentifiableShortCircuit.class);
            assertNotNull(identifiableShortCircuit);
            assertEquals(80., identifiableShortCircuit.getIpMin(), 0);
            assertEquals(220., identifiableShortCircuit.getIpMax(), 0);
            assertNotNull(vl.getExtensionByName(IdentifiableShortCircuit.NAME));
            assertEquals(IdentifiableShortCircuit.NAME, identifiableShortCircuit.getName());

            identifiableShortCircuit.setIpMin(90.);
            identifiableShortCircuit.setIpMax(260.);
            assertEquals(90., identifiableShortCircuit.getIpMin(), 0);
            assertEquals(260., identifiableShortCircuit.getIpMax(), 0);
        }
    }

    @Test
    public void cgmesExtensionsTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store
            Network network = service.importNetwork(CgmesConformity1Catalog.miniNodeBreaker().dataSource());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());

            // get extensions by name
            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());
            CgmesMetadataModels cgmesMetadataModels = readNetwork.getExtensionByName("cgmesMetadataModels");
            CimCharacteristics cimCharacteristics = readNetwork.getExtensionByName("cimCharacteristics");

            Optional<CgmesMetadataModel> cgmesSvMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STATE_VARIABLES);
            assertTrue(cgmesSvMetadataModel.isPresent());
            CgmesMetadataModel cgmesSvMetadata = cgmesSvMetadataModel.get();
            assertEquals(573, cgmesSvMetadata.getDescription().length());
            assertTrue(cgmesSvMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSvMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSvMetadata.getModelingAuthoritySet());
            assertEquals(3, cgmesSvMetadata.getDependentOn().size());

            Optional<CgmesMetadataModel> cgmesSshMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS);
            assertTrue(cgmesSshMetadataModel.isPresent());
            CgmesMetadataModel cgmesSshMetadata = cgmesSshMetadataModel.get();
            assertEquals(573, cgmesSshMetadata.getDescription().length());
            assertTrue(cgmesSshMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSshMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSshMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSshMetadata.getDependentOn().size());

            assertEquals(CgmesTopologyKind.NODE_BREAKER, cimCharacteristics.getTopologyKind());
            assertEquals(16, cimCharacteristics.getCimVersion());

            // get extensions by class
            cgmesMetadataModels = readNetwork.getExtension(CgmesMetadataModels.class);
            cimCharacteristics = readNetwork.getExtension(CimCharacteristics.class);
            cgmesSvMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STATE_VARIABLES);

            assertTrue(cgmesSvMetadataModel.isPresent());
            cgmesSvMetadata = cgmesSvMetadataModel.get();
            assertEquals(573, cgmesSvMetadata.getDescription().length());
            assertTrue(cgmesSvMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSvMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSvMetadata.getModelingAuthoritySet());
            assertEquals(3, cgmesSvMetadata.getDependentOn().size());

            cgmesSshMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS);
            assertTrue(cgmesSshMetadataModel.isPresent());
            cgmesSshMetadata = cgmesSshMetadataModel.get();
            assertEquals(573, cgmesSshMetadata.getDescription().length());
            assertTrue(cgmesSshMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSshMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSshMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSshMetadata.getDependentOn().size());

            assertEquals(CgmesTopologyKind.NODE_BREAKER, cimCharacteristics.getTopologyKind());
            assertEquals(16, cimCharacteristics.getCimVersion());

            // get all extensions
            Collection<Extension<Network>> cgmesExtensions = readNetwork.getExtensions();
            Iterator<Extension<Network>> it = cgmesExtensions.iterator();
            cgmesMetadataModels = (CgmesMetadataModels) it.next();
            cimCharacteristics = (CimCharacteristics) it.next();

            cgmesSvMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STATE_VARIABLES);
            assertTrue(cgmesSvMetadataModel.isPresent());
            cgmesSvMetadata = cgmesSvMetadataModel.get();
            assertEquals(573, cgmesSvMetadata.getDescription().length());
            assertTrue(cgmesSvMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSvMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSvMetadata.getModelingAuthoritySet());
            assertEquals(3, cgmesSvMetadata.getDependentOn().size());

            cgmesSshMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS);
            assertTrue(cgmesSshMetadataModel.isPresent());
            cgmesSshMetadata = cgmesSshMetadataModel.get();
            assertEquals(573, cgmesSshMetadata.getDescription().length());
            assertTrue(cgmesSshMetadata.getDescription().contains("CGMES Conformity Assessment"));
            assertEquals(4, cgmesSshMetadata.getVersion());
            assertEquals("http://A1.de/Planning/ENTSOE/2", cgmesSshMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSshMetadata.getDependentOn().size());

            assertEquals(CgmesTopologyKind.NODE_BREAKER, cimCharacteristics.getTopologyKind());
            assertEquals(16, cimCharacteristics.getCimVersion());

            // modify extensions
            CgmesMetadataModelsAttributes cgmesMetadataModelAttributes = CgmesMetadataModelsAttributes.builder()
                    .models(List.of(new CgmesMetadataModelAttributes(CgmesSubset.STATE_VARIABLES,
                                    "svId", "DescriptionSv", 6, "modelingAuthoritySetSv", List.of("profileSv"),
                                    List.of("dependentOnSv"), List.of("supersedesSv")),
                            new CgmesMetadataModelAttributes(CgmesSubset.STEADY_STATE_HYPOTHESIS,
                                    "sshId", "DescriptionSsh", 7, "modelingAuthoritySetSsh", List.of("profileSsh"),
                                    List.of("dependentOnSsh"), List.of("supersedesSsh"))))
                    .build();

            ((NetworkImpl) readNetwork).getResource().getAttributes().getExtensionAttributes().put(CgmesMetadataModels.NAME, cgmesMetadataModelAttributes);

            CimCharacteristicsAttributes cimCharacteristicsAttributes = CimCharacteristicsAttributes.builder()
                    .cimVersion(5)
                    .cgmesTopologyKind(CgmesTopologyKind.BUS_BRANCH)
                    .build();

            ((NetworkImpl) readNetwork).updateResource(res -> res.getAttributes().setCimCharacteristics(cimCharacteristicsAttributes));

            service.flush(readNetwork);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());

            // get extensions by name
            CgmesMetadataModels cgmesMetadataModels = readNetwork.getExtensionByName("cgmesMetadataModels");
            Optional<CgmesMetadataModel> cgmesSvMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STATE_VARIABLES);
            assertTrue(cgmesSvMetadataModel.isPresent());
            CgmesMetadataModel cgmesSvMetadata = cgmesSvMetadataModel.get();
            assertEquals("svId", cgmesSvMetadata.getId());
            assertEquals("DescriptionSv", cgmesSvMetadata.getDescription());
            assertEquals(6, cgmesSvMetadata.getVersion());
            assertEquals("modelingAuthoritySetSv", cgmesSvMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSvMetadata.getProfiles().size());
            assertTrue(cgmesSvMetadata.getProfiles().contains("profileSv"));
            assertEquals(1, cgmesSvMetadata.getDependentOn().size());
            assertTrue(cgmesSvMetadata.getDependentOn().contains("dependentOnSv"));
            assertEquals(1, cgmesSvMetadata.getSupersedes().size());
            assertTrue(cgmesSvMetadata.getSupersedes().contains("supersedesSv"));

            Optional<CgmesMetadataModel> cgmesSshMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS);
            assertTrue(cgmesSshMetadataModel.isPresent());
            CgmesMetadataModel cgmesSshMetadata = cgmesSshMetadataModel.get();
            assertEquals("sshId", cgmesSshMetadata.getId());
            assertEquals("DescriptionSsh", cgmesSshMetadata.getDescription());
            assertEquals(7, cgmesSshMetadata.getVersion());
            assertEquals("modelingAuthoritySetSsh", cgmesSshMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSshMetadata.getProfiles().size());
            assertTrue(cgmesSshMetadata.getProfiles().contains("profileSsh"));
            assertEquals(1, cgmesSshMetadata.getDependentOn().size());
            assertTrue(cgmesSshMetadata.getDependentOn().contains("dependentOnSsh"));
            assertEquals(1, cgmesSshMetadata.getSupersedes().size());
            assertTrue(cgmesSshMetadata.getSupersedes().contains("supersedesSsh"));

            CimCharacteristics cimCharacteristics = readNetwork.getExtensionByName("cimCharacteristics");
            assertEquals(CgmesTopologyKind.BUS_BRANCH, cimCharacteristics.getTopologyKind());
            assertEquals(5, cimCharacteristics.getCimVersion());

            // create new extensions
            readNetwork.newExtension(CgmesMetadataModelsAdder.class)
                    .newModel()
                    .setSubset(CgmesSubset.STATE_VARIABLES)
                    .setId("svId2")
                    .setDescription("DescriptionSv2")
                    .setVersion(9)
                    .setModelingAuthoritySet("modelingAuthoritySetSv2")
                    .addProfile("profileSv2")
                    .addDependentOn("dependentOnSv2")
                    .addSupersedes("supersedesSv2")
                    .add()
                    .newModel()
                    .setSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS)
                    .setId("sshId2")
                    .setDescription("DescriptionSsh2")
                    .setVersion(3)
                    .setModelingAuthoritySet("modelingAuthoritySetSsh2")
                    .addProfile("profileSsh2")
                    .addDependentOn("dependentOnSsh2")
                    .addSupersedes("supersedesSsh2")
                    .add()
                    .add();
            readNetwork.newExtension(CimCharacteristicsAdder.class)
                    .setTopologyKind(CgmesTopologyKind.NODE_BREAKER)
                    .setCimVersion(6)
                    .add();
            service.flush(readNetwork);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());

            // get extensions by name
            CgmesMetadataModels cgmesMetadataModels = readNetwork.getExtensionByName("cgmesMetadataModels");
            Optional<CgmesMetadataModel> cgmesSvMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STATE_VARIABLES);
            assertTrue(cgmesSvMetadataModel.isPresent());
            CgmesMetadataModel cgmesSvMetadata = cgmesSvMetadataModel.get();
            assertEquals("svId2", cgmesSvMetadata.getId());
            assertEquals("DescriptionSv2", cgmesSvMetadata.getDescription());
            assertEquals(9, cgmesSvMetadata.getVersion());
            assertEquals("modelingAuthoritySetSv2", cgmesSvMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSvMetadata.getProfiles().size());
            assertTrue(cgmesSvMetadata.getProfiles().contains("profileSv2"));
            assertEquals(1, cgmesSvMetadata.getDependentOn().size());
            assertTrue(cgmesSvMetadata.getDependentOn().contains("dependentOnSv2"));
            assertEquals(1, cgmesSvMetadata.getSupersedes().size());
            assertTrue(cgmesSvMetadata.getSupersedes().contains("supersedesSv2"));

            Optional<CgmesMetadataModel> cgmesSshMetadataModel = cgmesMetadataModels.getModelForSubset(CgmesSubset.STEADY_STATE_HYPOTHESIS);
            assertTrue(cgmesSshMetadataModel.isPresent());
            CgmesMetadataModel cgmesSshMetadata = cgmesSshMetadataModel.get();
            assertEquals("sshId2", cgmesSshMetadata.getId());
            assertEquals("DescriptionSsh2", cgmesSshMetadata.getDescription());
            assertEquals(3, cgmesSshMetadata.getVersion());
            assertEquals("modelingAuthoritySetSsh2", cgmesSshMetadata.getModelingAuthoritySet());
            assertEquals(1, cgmesSshMetadata.getProfiles().size());
            assertTrue(cgmesSshMetadata.getProfiles().contains("profileSsh2"));
            assertEquals(1, cgmesSshMetadata.getDependentOn().size());
            assertTrue(cgmesSshMetadata.getDependentOn().contains("dependentOnSsh2"));
            assertEquals(1, cgmesSshMetadata.getSupersedes().size());
            assertTrue(cgmesSshMetadata.getSupersedes().contains("supersedesSsh2"));

            CimCharacteristics cimCharacteristics = readNetwork.getExtensionByName("cimCharacteristics");
            assertEquals(CgmesTopologyKind.NODE_BREAKER, cimCharacteristics.getTopologyKind());
            assertEquals(6, cimCharacteristics.getCimVersion());
        }
    }

    private Network createExtensionsNetwork(NetworkFactory networkFactory) {
        Network network = networkFactory.createNetwork("Extensions network", "test");

        Substation s1 = createSubstation(network, "s1", "s1", Country.FR);
        VoltageLevel v1 = createVoltageLevel(s1, "v1", "v1", TopologyKind.NODE_BREAKER, 380.0);
        createBusBarSection(v1, "1.1", "1.1", 0, 1, 1);
        createLoad(v1, "v1load", "v1load", "v1load", 1, ConnectablePosition.Direction.TOP, 2, 0., 0.);

        VoltageLevel v2 = createVoltageLevel(s1, "v2", "v2", TopologyKind.NODE_BREAKER, 225.0);
        createBusBarSection(v2, "2.1", "2.1", 0, 1, 1);

        VoltageLevel v3 = createVoltageLevel(s1, "v3", "v3", TopologyKind.NODE_BREAKER, 100.0);
        createBusBarSection(v3, "3.1", "3.1", 0, 1, 1);

        TwoWindingsTransformer twt2 = s1.newTwoWindingsTransformer().setId("TWT2")
                .setName("My two windings transformer").setVoltageLevel1("v1").setVoltageLevel2("v2").setNode1(1)
                .setNode2(1).setR(0.5).setX(4).setG(0).setB(0).setRatedU1(24).setRatedU2(385).setRatedS(100).add();
        twt2.newExtension(ConnectablePositionAdder.class).newFeeder1().withName("twt2.1").withOrder(2)
                .withDirection(ConnectablePosition.Direction.TOP).add().newFeeder2().withName("twt2.2").withOrder(2)
                .withDirection(ConnectablePosition.Direction.TOP).add().add();
        ConnectablePosition cptwt2 = twt2.getExtension(ConnectablePosition.class);
        assertEquals("twt2.1", cptwt2.getFeeder1().getName().orElseThrow());
        assertEquals(2, cptwt2.getFeeder1().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder1().getDirection());
        assertEquals("twt2.2", cptwt2.getFeeder2().getName().orElseThrow());
        assertEquals(2, cptwt2.getFeeder2().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder2().getDirection());
        cptwt2 = twt2.getExtensionByName("position");
        assertEquals("twt2.1", cptwt2.getFeeder1().getName().orElseThrow());
        assertEquals(2, cptwt2.getFeeder1().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder1().getDirection());
        assertEquals("twt2.2", cptwt2.getFeeder2().getName().orElseThrow());
        assertEquals(2, cptwt2.getFeeder2().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder2().getDirection());

        ThreeWindingsTransformer twt3 = s1.newThreeWindingsTransformer().setId("TWT3")
                .setName("Three windings transformer 1").setRatedU0(234).newLeg1().setVoltageLevel("v1").setNode(1)
                .setR(45).setX(35).setG(25).setB(15).setRatedU(5).add().newLeg2().setVoltageLevel("v2").setNode(1)
                .setR(47).setX(37).setG(27).setB(17).setRatedU(7).add().newLeg3().setVoltageLevel("v3").setNode(1)
                .setR(49).setX(39).setG(29).setB(19).setRatedU(9).add().add();
        twt3.newExtension(ConnectablePositionAdder.class).newFeeder1().withName("twt3.1").withOrder(3)
                .withDirection(ConnectablePosition.Direction.BOTTOM).add().newFeeder2().withName("twt3.2").withOrder(3)
                .withDirection(ConnectablePosition.Direction.BOTTOM).add().newFeeder3().withName("twt3.3").withOrder(3)
                .withDirection(ConnectablePosition.Direction.BOTTOM).add().add();

        ConnectablePosition cptwt3 = twt3.getExtension(ConnectablePosition.class);
        assertEquals("twt3.1", cptwt3.getFeeder1().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder1().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder1().getDirection());
        assertEquals("twt3.2", cptwt3.getFeeder2().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder2().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder2().getDirection());
        assertEquals("twt3.3", cptwt3.getFeeder3().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder3().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder3().getDirection());
        cptwt3 = twt3.getExtensionByName("position");
        assertEquals("twt3.1", cptwt3.getFeeder1().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder1().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder1().getDirection());
        assertEquals("twt3.2", cptwt3.getFeeder2().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder2().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder2().getDirection());
        assertEquals("twt3.3", cptwt3.getFeeder3().getName().orElseThrow());
        assertEquals(3, cptwt3.getFeeder3().getOrder().orElseThrow().intValue());
        assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder3().getDirection());
        return network;
    }

    @Test
    public void extensionsTest() {
        // create network and save it
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            service.flush(createExtensionsNetwork(service.getNetworkFactory()));
        }

        // load saved network
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());
            assertEquals("Extensions network", readNetwork.getId());

            Load load = readNetwork.getLoad("v1load");
            TwoWindingsTransformer twt2 = readNetwork.getTwoWindingsTransformer("TWT2");
            ThreeWindingsTransformer twt3 = readNetwork.getThreeWindingsTransformer("TWT3");
            assertNotNull(load);
            assertNotNull(twt2);
            assertNotNull(twt3);
            ConnectablePosition cpload = load.getExtension(ConnectablePosition.class);
            assertNotNull(cpload);
            assertEquals("v1load", cpload.getFeeder().getName().orElseThrow());
            assertEquals(1, cpload.getFeeder().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cpload.getFeeder().getDirection());
            cpload = load.getExtensionByName("position");
            assertNotNull(cpload);
            assertEquals("v1load", cpload.getFeeder().getName().orElseThrow());
            assertEquals(1, cpload.getFeeder().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cpload.getFeeder().getDirection());

            ConnectablePosition cptwt2 = twt2.getExtension(ConnectablePosition.class);
            assertNotNull(cptwt2);
            assertEquals("twt2.1", cptwt2.getFeeder1().getName().orElseThrow());
            assertEquals(2, cptwt2.getFeeder1().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder1().getDirection());
            assertEquals("twt2.2", cptwt2.getFeeder2().getName().orElseThrow());
            assertEquals(2, cptwt2.getFeeder2().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder2().getDirection());
            cptwt2 = twt2.getExtensionByName("position");
            assertNotNull(cptwt2);
            assertEquals("twt2.1", cptwt2.getFeeder1().getName().orElseThrow());
            assertEquals(2, cptwt2.getFeeder1().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder1().getDirection());
            assertEquals("twt2.2", cptwt2.getFeeder2().getName().orElseThrow());
            assertEquals(2, cptwt2.getFeeder2().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.TOP, cptwt2.getFeeder2().getDirection());

            ConnectablePosition cptwt3 = twt3.getExtension(ConnectablePosition.class);
            assertNotNull(cptwt3);
            assertEquals("twt3.1", cptwt3.getFeeder1().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder1().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder1().getDirection());
            assertEquals("twt3.2", cptwt3.getFeeder2().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder2().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder2().getDirection());
            assertEquals("twt3.3", cptwt3.getFeeder3().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder3().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder3().getDirection());
            cptwt3 = twt3.getExtensionByName("position");
            assertNotNull(cptwt3);
            assertEquals("twt3.1", cptwt3.getFeeder1().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder1().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder1().getDirection());
            assertEquals("twt3.2", cptwt3.getFeeder2().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder2().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder2().getDirection());
            assertEquals("twt3.3", cptwt3.getFeeder3().getName().orElseThrow());
            assertEquals(3, cptwt3.getFeeder3().getOrder().orElseThrow().intValue());
            assertEquals(ConnectablePosition.Direction.BOTTOM, cptwt3.getFeeder3().getDirection());
        }
    }

    @Test
    public void coordinatedReactiveControlTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Generator gen = network.getGenerator("GEN");
            assertNull(gen.getExtension(CoordinatedReactiveControl.class));
            assertNull(gen.getExtensionByName("coordinatedReactiveControl"));
            assertTrue(gen.getExtensions().isEmpty());
            gen.newExtension(CoordinatedReactiveControlAdder.class)
                    .withQPercent(50)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            Generator gen = network.getGenerator("GEN");
            CoordinatedReactiveControl extension = gen.getExtension(CoordinatedReactiveControl.class);
            assertNotNull(extension);
            assertEquals(50, extension.getQPercent(), 0);
            assertNotNull(gen.getExtensionByName("coordinatedReactiveControl"));
            assertEquals(1, gen.getExtensions().size());
        }
    }

    @Test
    public void generatorEntsoeCategoryTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Generator gen = network.getGenerator("GEN");
            assertNull(gen.getExtension(GeneratorEntsoeCategory.class));
            assertNull(gen.getExtensionByName("entsoeCategory"));
            assertTrue(gen.getExtensions().isEmpty());
            gen.newExtension(GeneratorEntsoeCategoryAdder.class)
                    .withCode(50)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            Generator gen = network.getGenerator("GEN");
            GeneratorEntsoeCategory extension = gen.getExtension(GeneratorEntsoeCategory.class);
            assertNotNull(extension);
            assertEquals(50, extension.getCode());
            assertNotNull(gen.getExtensionByName("entsoeCategory"));
            assertEquals(1, gen.getExtensions().size());
        }
    }

    @Test
    public void voltagePerReactivePowerControlTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = SvcTestCaseFactory.create(service.getNetworkFactory());
            StaticVarCompensator svc2 = network.getStaticVarCompensator("SVC2");
            assertNull(svc2.getExtension(VoltagePerReactivePowerControl.class));
            assertNull(svc2.getExtensionByName("voltagePerReactivePowerControl"));
            assertTrue(svc2.getExtensions().isEmpty());
            svc2.newExtension(VoltagePerReactivePowerControlAdder.class)
                    .withSlope(0.3)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            StaticVarCompensator svc2 = network.getStaticVarCompensator("SVC2");
            VoltagePerReactivePowerControl extension = svc2.getExtension(VoltagePerReactivePowerControl.class);
            assertNotNull(extension);
            assertEquals(0.3, extension.getSlope(), 0);
            assertNotNull(svc2.getExtensionByName("voltagePerReactivePowerControl"));
            assertEquals(1, svc2.getExtensions().size());
        }
    }

    @Test
    public void loadDetailExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = SvcTestCaseFactory.create(service.getNetworkFactory());
            Load load2 = network.getLoad("L2");
            assertNull(load2.getExtension(LoadDetail.class));
            assertNull(load2.getExtensionByName("loadDetail"));
            assertTrue(load2.getExtensions().isEmpty());
            load2.newExtension(LoadDetailAdder.class)
                    .withFixedActivePower(5.5f)
                    .withFixedReactivePower(2.5f)
                    .withVariableActivePower(3.2f)
                    .withVariableReactivePower(2.1f)
                    .add();
            assertNotNull(load2.getExtension(LoadDetail.class));
            assertNotNull(load2.getExtensionByName("loadDetail"));
            assertFalse(load2.getExtensions().isEmpty());
            LoadDetail loadDetail = load2.getExtension(LoadDetail.class);
            assertEquals(5.5f, loadDetail.getFixedActivePower(), 0.1f);
            assertEquals(2.5f, loadDetail.getFixedReactivePower(), 0.1f);
            assertEquals(3.2f, loadDetail.getVariableActivePower(), 0.1f);
            assertEquals(2.1f, loadDetail.getVariableReactivePower(), 0.1f);
            service.flush(network);
            loadDetail.setFixedActivePower(7.5f);
            loadDetail.setFixedReactivePower(4.5f);
            loadDetail.setVariableActivePower(5.2f);
            loadDetail.setVariableReactivePower(4.1f);
            assertEquals(7.5f, loadDetail.getFixedActivePower(), 0.1f);
            assertEquals(4.5f, loadDetail.getFixedReactivePower(), 0.1f);
            assertEquals(5.2f, loadDetail.getVariableActivePower(), 0.1f);
            assertEquals(4.1f, loadDetail.getVariableReactivePower(), 0.1f);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            Load load2 = network.getLoad("L2");
            assertNotNull(load2.getExtension(LoadDetail.class));
            assertNotNull(load2.getExtensionByName("loadDetail"));
            assertFalse(load2.getExtensions().isEmpty());
            LoadDetail loadDetail = load2.getExtension(LoadDetail.class);
            assertEquals(5.5f, loadDetail.getFixedActivePower(), 0.1f);
            assertEquals(2.5f, loadDetail.getFixedReactivePower(), 0.1f);
            assertEquals(3.2f, loadDetail.getVariableActivePower(), 0.1f);
            assertEquals(2.1f, loadDetail.getVariableReactivePower(), 0.1f);
        }
    }

    @Test
    public void slackTerminalExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = SvcTestCaseFactory.create(service.getNetworkFactory());
            VoltageLevel vl = network.getVoltageLevel("VL1");
            assertNull(vl.getExtension(SlackTerminal.class));
            assertNull(vl.getExtensionByName("slackTerminal"));
            assertTrue(vl.getExtensions().isEmpty());
            SlackTerminalAdder slackTerminalAdder = vl.newExtension(SlackTerminalAdder.class).withTerminal(null);
            assertThrows(PowsyblException.class, slackTerminalAdder::add);
            assertNull(vl.getExtension(SlackTerminal.class));
            assertNull(vl.getExtensionByName("slackTerminal"));
            assertTrue(vl.getExtensions().isEmpty());
            Generator generator = network.getGenerator("G1");
            vl.newExtension(SlackTerminalAdder.class)
                    .withTerminal(generator.getTerminal())
                    .add();
            assertNotNull(vl.getExtension(SlackTerminal.class));
            assertNotNull(vl.getExtensionByName("slackTerminal"));
            assertFalse(vl.getExtensions().isEmpty());
            assertEquals(vl.getExtension(SlackTerminal.class).getTerminal(), generator.getTerminal());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            VoltageLevel vl = network.getVoltageLevel("VL1");
            Generator generator = network.getGenerator("G1");
            vl.newExtension(SlackTerminalAdder.class)
                    .withTerminal(generator.getTerminal())
                    .add();
            assertNotNull(vl.getExtension(SlackTerminal.class));
            assertNotNull(vl.getExtensionByName("slackTerminal"));
            assertFalse(vl.getExtensions().isEmpty());
            assertEquals(vl.getExtension(SlackTerminal.class).getTerminal(), generator.getTerminal());
        }
    }

    @Test
    public void threeWindingsTransformerPhaseAngleClockExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = ThreeWindingsTransformerNetworkFactory.create(service.getNetworkFactory());
            Substation substation = network.getSubstation("SUBSTATION");
            ThreeWindingsTransformer twt = substation.getThreeWindingsTransformerStream().findFirst().orElseThrow(AssertionError::new);
            assertNull(twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class));
            assertNull(twt.getExtensionByName("threeWindingsTransformerPhaseAngleClock"));
            assertTrue(twt.getExtensions().isEmpty());
            twt.newExtension(ThreeWindingsTransformerPhaseAngleClockAdder.class).withPhaseAngleClockLeg2(1).withPhaseAngleClockLeg3(2).add();
            assertNotNull(twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class));
            assertNotNull(twt.getExtensionByName("threeWindingsTransformerPhaseAngleClock"));
            assertFalse(twt.getExtensions().isEmpty());
            assertEquals(1, twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class).getPhaseAngleClockLeg2());
            assertEquals(2, twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class).getPhaseAngleClockLeg3());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            Substation substation = network.getSubstation("SUBSTATION");
            ThreeWindingsTransformer twt = substation.getThreeWindingsTransformerStream().findFirst().orElseThrow(AssertionError::new);
            assertNotNull(twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class));
            assertNotNull(twt.getExtensionByName("threeWindingsTransformerPhaseAngleClock"));
            assertFalse(twt.getExtensions().isEmpty());
            assertEquals(1, twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class).getPhaseAngleClockLeg2());
            assertEquals(2, twt.getExtension(ThreeWindingsTransformerPhaseAngleClock.class).getPhaseAngleClockLeg3());
        }
    }

    @Test
    public void twoWindingsTransformerPhaseAngleClockExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());

            TwoWindingsTransformer twt = network.getTwoWindingsTransformer("NGEN_NHV1");
            assertNull(twt.getExtension(TwoWindingsTransformerPhaseAngleClock.class));
            assertNull(twt.getExtensionByName("twoWindingsTransformerPhaseAngleClock"));
            assertTrue(twt.getExtensions().isEmpty());
            twt.newExtension(TwoWindingsTransformerPhaseAngleClockAdder.class).withPhaseAngleClock(1).add();
            assertNotNull(twt.getExtension(TwoWindingsTransformerPhaseAngleClock.class));
            assertNotNull(twt.getExtensionByName("twoWindingsTransformerPhaseAngleClock"));
            assertFalse(twt.getExtensions().isEmpty());
            assertEquals(1, twt.getExtension(TwoWindingsTransformerPhaseAngleClock.class).getPhaseAngleClock());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            TwoWindingsTransformer twt = network.getTwoWindingsTransformer("NGEN_NHV1");
            assertNotNull(twt.getExtension(TwoWindingsTransformerPhaseAngleClock.class));
            assertNotNull(twt.getExtensionByName("twoWindingsTransformerPhaseAngleClock"));
            assertFalse(twt.getExtensions().isEmpty());
            assertEquals(1, twt.getExtension(TwoWindingsTransformerPhaseAngleClock.class).getPhaseAngleClock());
        }
    }

    @Test
    public void svcVoltagePerReactivePowerExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = SvcTestCaseFactory.create(service.getNetworkFactory());

            StaticVarCompensator svc = network.getStaticVarCompensator("SVC2");
            assertNull(svc.getExtension(VoltagePerReactivePowerControl.class));
            assertNull(svc.getExtensionByName("voltagePerReactivePowerControl"));
            assertTrue(svc.getExtensions().isEmpty());
            svc.newExtension(VoltagePerReactivePowerControlAdder.class).withSlope(1).add();
            assertNotNull(svc.getExtension(VoltagePerReactivePowerControl.class));
            assertNotNull(svc.getExtensionByName("voltagePerReactivePowerControl"));
            assertFalse(svc.getExtensions().isEmpty());
            assertEquals(1, svc.getExtension(VoltagePerReactivePowerControl.class).getSlope(), 0.001);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            StaticVarCompensator svc = network.getStaticVarCompensator("SVC2");
            assertNotNull(svc.getExtension(VoltagePerReactivePowerControl.class));
            assertNotNull(svc.getExtensionByName("voltagePerReactivePowerControl"));
            assertFalse(svc.getExtensions().isEmpty());
            assertEquals(1, svc.getExtension(VoltagePerReactivePowerControl.class).getSlope(), 0.001);
        }
    }

    @Test
    public void coordinatedReactiveControlExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = SvcTestCaseFactory.create(service.getNetworkFactory());

            Generator generator = network.getGenerator("G1");
            assertNull(generator.getExtension(CoordinatedReactiveControl.class));
            assertNull(generator.getExtensionByName("coordinatedReactiveControl"));
            assertTrue(generator.getExtensions().isEmpty());
            generator.newExtension(CoordinatedReactiveControlAdder.class).withQPercent(1).add();
            assertNotNull(generator.getExtension(CoordinatedReactiveControl.class));
            assertNotNull(generator.getExtensionByName("coordinatedReactiveControl"));
            assertFalse(generator.getExtensions().isEmpty());
            assertEquals(1, generator.getExtension(CoordinatedReactiveControl.class).getQPercent(), 0.001);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));
            Generator generator = network.getGenerator("G1");
            assertNotNull(generator.getExtension(CoordinatedReactiveControl.class));
            assertNotNull(generator.getExtensionByName("coordinatedReactiveControl"));
            assertFalse(generator.getExtensions().isEmpty());
            assertEquals(1, generator.getExtension(CoordinatedReactiveControl.class).getQPercent(), 0.001);
        }
    }

    @Test
    public void batteryActivePowerControlTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = NetworkStorageTestCaseFactory.create(service.getNetworkFactory());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {

            Map<UUID, String> networkIds = service.getNetworkIds();

            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());

            assertEquals("networkTestCase", readNetwork.getId());

            assertEquals(1, readNetwork.getBatteryCount());

            Battery battery = readNetwork.getBattery("battery");

            battery.newExtension(ActivePowerControlAdder.class)
                    .withParticipate(false)
                    .withDroop(1.0f)
                    .add();
            ActivePowerControl activePowerControl = battery.getExtension(ActivePowerControl.class);
            assertFalse(activePowerControl.isParticipate());
            assertEquals(1.0f, activePowerControl.getDroop(), 0.01);

            activePowerControl = battery.getExtensionByName("activePowerControl");
            assertFalse(activePowerControl.isParticipate());
            assertEquals(1.0f, activePowerControl.getDroop(), 0.01);

            Collection<Extension<Battery>> extensions = battery.getExtensions();
            assertEquals(1, extensions.size());
            activePowerControl = (ActivePowerControl) extensions.iterator().next();
            assertFalse(activePowerControl.isParticipate());
            assertEquals(1.0f, activePowerControl.getDroop(), 0.01);
        }
    }

    @Test
    public void lccActivePowerControlTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = NetworkStorageTestCaseFactory.create(service.getNetworkFactory());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {

            Map<UUID, String> networkIds = service.getNetworkIds();

            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());

            assertEquals("networkTestCase", readNetwork.getId());

            LccConverterStation lccConverterStation = readNetwork.getLccConverterStation("LCC2");

            ActivePowerControlAdder activePowerControlAdder = lccConverterStation.newExtension(ActivePowerControlAdder.class).withParticipate(false).withDroop(1.0f);
            assertThrows(UnsupportedOperationException.class, activePowerControlAdder::add).getMessage().contains("Cannot set ActivePowerControl");
            assertNull(lccConverterStation.getExtension(ActivePowerControl.class));
        }
    }

    @Test
    public void loadActivePowerControlTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = NetworkStorageTestCaseFactory.create(service.getNetworkFactory());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {

            Map<UUID, String> networkIds = service.getNetworkIds();

            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());

            assertEquals("networkTestCase", readNetwork.getId());

            Load load = readNetwork.getLoad("load1");

            ActivePowerControlAdder activePowerControlAdder = load.newExtension(ActivePowerControlAdder.class)
                    .withParticipate(false)
                    .withDroop(1.0f);
            assertThrows(UnsupportedOperationException.class, activePowerControlAdder::add).getMessage().contains("Cannot set ActivePowerControl");
            assertNull(load.getExtension(ActivePowerControl.class));
        }
    }

    @Test
    public void hvdcAngleDroopActivePowerControlExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = HvdcTestNetwork.createVsc(service.getNetworkFactory());
            HvdcLine hvdcLine = network.getHvdcLine("L");
            assertNull(hvdcLine.getExtension(HvdcAngleDroopActivePowerControl.class));
            assertNull(hvdcLine.getExtensionByName("hvdcAngleDroopActivePowerControl"));
            assertTrue(hvdcLine.getExtensions().isEmpty());
            hvdcLine.newExtension(HvdcAngleDroopActivePowerControlAdder.class)
                    .withP0(10.0f)
                    .withDroop(5.0f)
                    .withEnabled(true)
                    .add();
            assertNotNull(hvdcLine.getExtension(HvdcAngleDroopActivePowerControl.class));
            assertNotNull(hvdcLine.getExtensionByName("hvdcAngleDroopActivePowerControl"));
            assertFalse(hvdcLine.getExtensions().isEmpty());
            HvdcAngleDroopActivePowerControl hvdcAngleDroopActivePowerControl = hvdcLine.getExtension(HvdcAngleDroopActivePowerControl.class);
            assertEquals(10.0f, hvdcAngleDroopActivePowerControl.getP0(), 0.1f);
            assertEquals(5.0f, hvdcAngleDroopActivePowerControl.getDroop(), 0.1f);
            assertTrue(hvdcAngleDroopActivePowerControl.isEnabled());

            service.flush(network);

            hvdcAngleDroopActivePowerControl.setP0(20.0f);
            hvdcAngleDroopActivePowerControl.setDroop(80.0f);
            hvdcAngleDroopActivePowerControl.setEnabled(false);
            assertEquals(20.0f, hvdcAngleDroopActivePowerControl.getP0(), 0.1f);
            assertEquals(80.0f, hvdcAngleDroopActivePowerControl.getDroop(), 0.1f);
            assertFalse(hvdcAngleDroopActivePowerControl.isEnabled());

            assertEquals("L", hvdcAngleDroopActivePowerControl.getExtendable().getId());
            assertThrows(IllegalArgumentException.class, () -> hvdcAngleDroopActivePowerControl.setP0(Float.NaN));
            assertThrows(IllegalArgumentException.class, () -> hvdcAngleDroopActivePowerControl.setDroop(Float.NaN));
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));

            HvdcLine hvdcLine = network.getHvdcLine("L");
            assertNotNull(hvdcLine.getExtension(HvdcAngleDroopActivePowerControl.class));
            assertNotNull(hvdcLine.getExtensionByName("hvdcAngleDroopActivePowerControl"));
            assertFalse(hvdcLine.getExtensions().isEmpty());

            HvdcAngleDroopActivePowerControl hvdcAngleDroopActivePowerControl = hvdcLine.getExtension(HvdcAngleDroopActivePowerControl.class);
            assertEquals(10.0f, hvdcAngleDroopActivePowerControl.getP0(), 0.1f);
            assertEquals(5.0f, hvdcAngleDroopActivePowerControl.getDroop(), 0.1f);
            assertTrue(hvdcAngleDroopActivePowerControl.isEnabled());
        }
    }

    @Test
    public void hvdcOperatorActivePowerRangeExtensionTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = HvdcTestNetwork.createVsc(service.getNetworkFactory());
            HvdcLine hvdcLine = network.getHvdcLine("L");
            assertNull(hvdcLine.getExtension(HvdcOperatorActivePowerRange.class));
            assertNull(hvdcLine.getExtensionByName("hvdcOperatorActivePowerRange"));
            assertTrue(hvdcLine.getExtensions().isEmpty());
            hvdcLine.newExtension(HvdcOperatorActivePowerRangeAdder.class)
                    .withOprFromCS1toCS2(15.0f)
                    .withOprFromCS2toCS1(8.0f)
                    .add();
            assertNotNull(hvdcLine.getExtension(HvdcOperatorActivePowerRange.class));
            assertNotNull(hvdcLine.getExtensionByName("hvdcOperatorActivePowerRange"));
            assertFalse(hvdcLine.getExtensions().isEmpty());
            HvdcOperatorActivePowerRange hvdcOperatorActivePowerRange = hvdcLine.getExtension(HvdcOperatorActivePowerRange.class);
            assertEquals(15.0f, hvdcOperatorActivePowerRange.getOprFromCS1toCS2(), 0.1f);
            assertEquals(8.0f, hvdcOperatorActivePowerRange.getOprFromCS2toCS1(), 0.1f);

            service.flush(network);

            hvdcOperatorActivePowerRange.setOprFromCS1toCS2(30.0f);
            hvdcOperatorActivePowerRange.setOprFromCS2toCS1(22.0f);
            assertEquals(30.0f, hvdcOperatorActivePowerRange.getOprFromCS1toCS2(), 0.1f);
            assertEquals(22.0f, hvdcOperatorActivePowerRange.getOprFromCS2toCS1(), 0.1f);

            assertEquals("L", hvdcOperatorActivePowerRange.getExtendable().getId());
            assertThrows(IllegalArgumentException.class, () -> hvdcOperatorActivePowerRange.setOprFromCS1toCS2(-1.0f));
            assertThrows(IllegalArgumentException.class, () -> hvdcOperatorActivePowerRange.setOprFromCS2toCS1(-2.0f));
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            Network network = service.getNetwork(networkIds.keySet().stream().findFirst().orElseThrow(AssertionError::new));

            HvdcLine hvdcLine = network.getHvdcLine("L");
            assertNotNull(hvdcLine.getExtension(HvdcOperatorActivePowerRange.class));
            assertNotNull(hvdcLine.getExtensionByName("hvdcOperatorActivePowerRange"));
            assertFalse(hvdcLine.getExtensions().isEmpty());

            HvdcOperatorActivePowerRange hvdcOperatorActivePowerRange = hvdcLine.getExtension(HvdcOperatorActivePowerRange.class);
            assertEquals(15.0f, hvdcOperatorActivePowerRange.getOprFromCS1toCS2(), 0.1f);
            assertEquals(8.0f, hvdcOperatorActivePowerRange.getOprFromCS2toCS1(), 0.1f);
        }
    }

    @Test
    public void cgmesControlAreaDanglingLineTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store
            Network network = service.importNetwork(CgmesConformity1Catalog.microGridBaseCaseBE().dataSource());
            CgmesControlAreas cgmesControlAreas = network.getExtension(CgmesControlAreas.class);
            assertNotNull(cgmesControlAreas);
            assertEquals(0, cgmesControlAreas.getCgmesControlAreas().size());
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            UUID networkUuid = networkIds.keySet().iterator().next();

            Network network = service.getNetwork(networkUuid);
            CgmesControlAreas cgmesControlAreas = network.getExtension(CgmesControlAreas.class);
            assertNotNull(cgmesControlAreas);
            assertEquals(0, cgmesControlAreas.getCgmesControlAreas().size());
            CgmesControlArea cgmesControlArea = cgmesControlAreas.newCgmesControlArea()
                    .setId("ca1")
                    .setEnergyIdentificationCodeEic("code")
                    .setNetInterchange(1000)
                    .add();
            cgmesControlArea.add(network.getGenerator("550ebe0d-f2b2-48c1-991f-cebea43a21aa").getTerminal());
            cgmesControlArea.add(network.getDanglingLine("a16b4a6c-70b1-4abf-9a9d-bd0fa47f9fe4").getBoundary());
            assertEquals(1, cgmesControlAreas.getCgmesControlAreas().size());
            CgmesControlArea ca1 = cgmesControlAreas.getCgmesControlArea("ca1");
            assertNotNull(ca1);

            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            UUID networkUuid = networkIds.keySet().iterator().next();

            Network network = service.getNetwork(networkUuid);
            CgmesControlAreas cgmesControlAreas = network.getExtension(CgmesControlAreas.class);
            assertNotNull(cgmesControlAreas);
            assertEquals(1, cgmesControlAreas.getCgmesControlAreas().size());
            assertTrue(cgmesControlAreas.containsCgmesControlAreaId("ca1"));
            CgmesControlArea cgmesControlArea = cgmesControlAreas.getCgmesControlArea("ca1");
            assertEquals("ca1", cgmesControlArea.getId());
            assertNull(cgmesControlArea.getName());
            assertEquals("code", cgmesControlArea.getEnergyIdentificationCodeEIC());
            assertEquals(1000, cgmesControlArea.getNetInterchange(), 0);
            assertEquals(1, cgmesControlArea.getTerminals().size());
            assertEquals(1, cgmesControlArea.getBoundaries().size());
        }
    }

    @Test
    public void baseVoltageMappingTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store
            Network network = service.importNetwork(CgmesConformity1Catalog.microGridBaseCaseBE().dataSource());
            assertNull(network.getExtension(Object.class));
            assertNull(network.getExtensionByName(""));
            BaseVoltageMapping baseVoltageMapping = network.getExtension(BaseVoltageMapping.class);
            assertNotNull(baseVoltageMapping);
            assertEquals(7, baseVoltageMapping.getBaseVoltages().size());
            assertFalse(baseVoltageMapping.isBaseVoltageEmpty());
            var ht = baseVoltageMapping.getBaseVoltage(400.0);
            assertNotNull(ht);
            assertEquals("65dd04e792584b3b912374e35dec032e", ht.getId());
            assertEquals(Source.BOUNDARY, ht.getSource());
            assertEquals(400.0, ht.getNominalV(), .1);

            assertFalse(baseVoltageMapping.isBaseVoltageMapped(42.0));
            assertNull(baseVoltageMapping.getBaseVoltage(42.0));
            // IGN do not remplace IGM
            baseVoltageMapping.addBaseVoltage(10.5, "somethingIGM", Source.IGM);
            assertNotEquals("somethingIGM", baseVoltageMapping.getBaseVoltage(10.5).getId());
            // BOUNDARY replace IGM
            baseVoltageMapping.addBaseVoltage(10.5, "something", Source.BOUNDARY);
            var bvs = BaseVoltageSourceAttribute.builder().id("something").nominalV(10.5).source(Source.BOUNDARY).build();
            assertEquals(bvs, baseVoltageMapping.getBaseVoltage(10.5));
            // BOUNDARY do not replace BOUNDARY
            baseVoltageMapping.addBaseVoltage(10.5, "somethingAgain", Source.BOUNDARY);
            assertNotEquals("somethingAgain", baseVoltageMapping.getBaseVoltage(10.5).getId());
            // IGM do not replace BOUNDARY
            baseVoltageMapping.addBaseVoltage(10.5, "somethingIGM", Source.IGM);
            assertNotEquals("somethingIGM", baseVoltageMapping.getBaseVoltage(10.5).getId());

            baseVoltageMapping.addBaseVoltage(42.0, "somethingElse", Source.IGM);
            var ft = baseVoltageMapping.getBaseVoltage(42.0);
            assertEquals("somethingElse", ft.getId());
            assertEquals(Source.IGM, ft.getSource());

            var baseVolatge = baseVoltageMapping.baseVoltagesByNominalVoltageMap();
            assertEquals(baseVoltageMapping.getBaseVoltages().size(), baseVolatge.size());
            service.flush(network);

        }
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            UUID networkUuid = networkIds.keySet().iterator().next();

            Network network = service.getNetwork(networkUuid);
            BaseVoltageMapping baseVoltageMapping = network.getExtensionByName("baseVoltageMapping");

            var ft = baseVoltageMapping.getBaseVoltage(42.0);
            assertNotNull(ft);
        }
    }

    @Test
    public void cgmesControlAreaTieLineTest() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store
            Properties properties = new Properties();
            properties.put(CgmesImport.IMPORT_CGM_WITH_SUBNETWORKS, "false");
            service.importNetwork(CgmesConformity1Catalog.microGridBaseCaseAssembled().dataSource(), null, LocalComputationManager.getDefault(), properties);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            UUID networkUuid = networkIds.keySet().iterator().next();

            Network network = service.getNetwork(networkUuid);
            CgmesControlAreas cgmesControlAreas = network.getExtension(CgmesControlAreas.class);
            assertNotNull(cgmesControlAreas);

            assertNotNull(cgmesControlAreas);
            assertEquals(0, cgmesControlAreas.getCgmesControlAreas().size());
            CgmesControlArea cgmesControlArea = cgmesControlAreas.newCgmesControlArea()
                    .setId("ca2")
                    .setEnergyIdentificationCodeEic("code2")
                    .setNetInterchange(800)
                    .add();
            cgmesControlArea.add(((TieLine) network.getTieLine("b18cd1aa-7808-49b9-a7cf-605eaf07b006 + e8acf6b6-99cb-45ad-b8dc-16c7866a4ddc")).getDanglingLine1().getBoundary());
            assertEquals(1, cgmesControlAreas.getCgmesControlAreas().size());

            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());
            UUID networkUuid = networkIds.keySet().iterator().next();

            Network network = service.getNetwork(networkUuid);
            CgmesControlAreas cgmesControlAreas = network.getExtension(CgmesControlAreas.class);
            assertNotNull(cgmesControlAreas);

            assertEquals(1, cgmesControlAreas.getCgmesControlAreas().size());
            CgmesControlArea cgmesControlArea = cgmesControlAreas.getCgmesControlArea("ca2");
            assertNotNull(cgmesControlArea);
            assertEquals(1, cgmesControlArea.getBoundaries().size());
        }
    }

    @Test
    public void testRemoteReactivePowerControl() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store
            Network network = service.importNetwork(CgmesConformity1ModifiedCatalog.microGridBaseCaseBEReactivePowerGen().dataSource());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {

            Map<UUID, String> networkIds = service.getNetworkIds();

            assertEquals(1, networkIds.size());

            Network readNetwork = service.getNetwork(networkIds.keySet().stream().findFirst().get());
            Generator g = readNetwork.getGenerator("3a3b27be-b18b-4385-b557-6735d733baf0");
            RemoteReactivePowerControl ext = g.getExtension(RemoteReactivePowerControl.class);
            assertNotNull(ext);
            assertEquals(115.5, ext.getTargetQ(), 0.0);
            assertTrue(ext.isEnabled());
            assertSame(readNetwork.getTwoWindingsTransformer("a708c3bc-465d-4fe7-b6ef-6fa6408a62b0").getTerminal2(), ext.getRegulatingTerminal());
        }
    }

    @Test
    public void testSubstationPosition() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Substation substation = network.getSubstation("P1");
            substation.newExtension(SubstationPositionAdder.class).withCoordinate(new Coordinate(48.0D, 2.0D)).add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            assertEquals(1, service.getNetworkIds().size());
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            SubstationPosition substationPosition = network.getSubstation("P1").getExtension(SubstationPosition.class);
            assertNotNull(substationPosition);
            assertEquals(new Coordinate(48.0D, 2.0D), substationPosition.getCoordinate());
        }
    }

    @Test
    public void testLinePosition() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            Line line = network.getLine("NHV1_NHV2_1");
            DanglingLine danglingLine = network.getVoltageLevel("VLGEN").newDanglingLine().setId("D_LINE")
                    .setB(33.4)
                    .setP0(66.9)
                    .setQ0(55.0)
                    .setR(7.0)
                    .setG(34.5)
                    .setX(23.7)
                    .setConnectableBus("NGEN")
                    .add();

            line.newExtension(LinePositionAdder.class).withCoordinates(List.of(new Coordinate(48.0D, 2.0D), new Coordinate(46.5D, 3.0D))).add();
            danglingLine.newExtension(LinePositionAdder.class).withCoordinates(List.of(new Coordinate(49.5D, 1.5D), new Coordinate(40.5D, 3.5D))).add();
            assertThrows(PowsyblException.class, () -> network.getVoltageLevel("VLGEN").newExtension(LinePositionAdder.class).withCoordinates(List.of(new Coordinate(48.0D, 2.0D), new Coordinate(46.5D, 3.0D))).add());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            assertEquals(1, service.getNetworkIds().size());
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());

            LinePosition<Line> linePosition = network.getLine("NHV1_NHV2_1").getExtension(LinePosition.class);
            assertNotNull(linePosition);
            assertEquals(List.of(new Coordinate(48.0D, 2.0D), new Coordinate(46.5D, 3.0D)), linePosition.getCoordinates());

            LinePosition<DanglingLine> danglingLinePosition = network.getDanglingLine("D_LINE").getExtension(LinePosition.class);
            assertNotNull(danglingLinePosition);
            assertEquals(List.of(new Coordinate(49.5D, 1.5D), new Coordinate(40.5D, 3.5D)), danglingLinePosition.getCoordinates());
        }
    }

    @Test
    public void testNetworkExtension() {
        String filePath = "/network_test1_cgmes_metadata_models.xml";
        ReadOnlyDataSource dataSource = getResource(filePath, filePath);

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            // import new network in the store with extension cgmesMetadataModels
            Network network = service.importNetwork(dataSource);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Map<UUID, String> networkIds = service.getNetworkIds();
            assertEquals(1, networkIds.size());

            // check that the stored network contains the extension cgmesMetadataModels
            UUID initialNetworkUuid = networkIds.keySet().stream().findFirst().get();
            Network readNetwork = service.getNetwork(initialNetworkUuid);
            assertNotNull(readNetwork.getExtensionByName("cgmesMetadataModels"));

            // clone network
            Network clonedNetwork = service.cloneNetwork(initialNetworkUuid, List.of("InitialState"));
            UUID clonedNetworkUuid = service.getNetworkUuid(clonedNetwork);
            Network readClonedNetwork = service.getNetwork(clonedNetworkUuid);
            assertNotNull(readClonedNetwork.getExtensionByName("cgmesMetadataModels"));

            service.deleteAllNetworks();
        }
    }

    @Test
    public void testMeasurements() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            twoWindingsTransformer.newExtension(MeasurementsAdder.class).add();
            twoWindingsTransformer.getExtension(Measurements.class)
                    .newMeasurement()
                    .setId("Measurement_ID_1")
                    .setValue(10)
                    .setValid(true)
                    .putProperty("source", "test")
                    .putProperty("other", "test3")
                    .setSide(ThreeSides.ONE)
                    .setStandardDeviation(1)
                    .setType(Measurement.Type.CURRENT)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());

            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            Measurements measurements = twoWindingsTransformer.getExtension(Measurements.class);
            assertNotNull(measurements);

            assertNull(measurements.getMeasurement("Measurement_ID_2"));

            Measurement measurement = measurements.getMeasurement("Measurement_ID_1");
            assertNotNull(measurement);
            assertEquals(Measurement.Type.CURRENT, measurement.getType());
            assertEquals(10, measurement.getValue(), 0.0);
            assertEquals(ThreeSides.ONE, measurement.getSide());
            assertEquals(1, measurement.getStandardDeviation(), 0.0);
            assertEquals("test", measurement.getProperty("source"));
            assertEquals("test3", measurement.getProperty("other"));
            assertNull(measurement.getProperty("doesnt exist"));

            assertEquals(1, measurements.getMeasurements(Measurement.Type.CURRENT).size());
            Measurement measurementFromType = (Measurement) measurements.getMeasurements(Measurement.Type.CURRENT).stream().findFirst().orElseThrow();
            assertEquals("Measurement_ID_1", measurementFromType.getId());
            assertEquals(Measurement.Type.CURRENT, measurementFromType.getType());
            measurementFromType.setStandardDeviation(2);
            measurementFromType.setValue(12);
            measurementFromType.setValid(false);
            measurementFromType.removeProperty("other");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            Measurements measurements = twoWindingsTransformer.getExtension(Measurements.class);
            assertNotNull(measurements);

            Measurement measurement = measurements.getMeasurement("Measurement_ID_1");
            assertNotNull(measurement);
            assertEquals(Measurement.Type.CURRENT, measurement.getType());
            assertEquals(12, measurement.getValue(), 0.0);
            assertEquals(2, measurement.getStandardDeviation(), 0.0);
            assertFalse(measurement.isValid());
            assertNull(measurement.getProperty("other"));
            assertNotNull(measurement.getProperty("source"));
            measurement.remove();

            measurement = measurements.getMeasurement("Measurement_ID_1");
            assertNull(measurement);
        }
    }

    @Test
    public void testDiscreteMeasurements() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.create(service.getNetworkFactory());
            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            twoWindingsTransformer.newExtension(DiscreteMeasurementsAdder.class).add();
            twoWindingsTransformer.getExtension(DiscreteMeasurements.class)
                    .newDiscreteMeasurement()
                    .setId("Measurement_ID_1")
                    .setValue(10)
                    .setValid(true)
                    .putProperty("source", "test")
                    .putProperty("other", "test3")
                    .setTapChanger(DiscreteMeasurement.TapChanger.PHASE_TAP_CHANGER)
                    .setType(DiscreteMeasurement.Type.TAP_POSITION)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());

            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            DiscreteMeasurements measurements = twoWindingsTransformer.getExtension(DiscreteMeasurements.class);
            assertNotNull(measurements);

            assertNull(measurements.getDiscreteMeasurement("Measurement_ID_2"));

            DiscreteMeasurement measurement = measurements.getDiscreteMeasurement("Measurement_ID_1");
            assertNotNull(measurement);
            assertEquals(DiscreteMeasurement.Type.TAP_POSITION, measurement.getType());
            assertEquals(10, measurement.getValueAsInt(), 0.0);
            assertEquals(DiscreteMeasurement.ValueType.INT, measurement.getValueType());
            assertTrue(measurement.isValid());
            assertEquals("test", measurement.getProperty("source"));
            assertEquals("test3", measurement.getProperty("other"));
            assertEquals(DiscreteMeasurement.TapChanger.PHASE_TAP_CHANGER, measurement.getTapChanger());
            assertNull(measurement.getProperty("doesnt exist"));

            assertEquals(1, measurements.getDiscreteMeasurements(DiscreteMeasurement.Type.TAP_POSITION).size());
            DiscreteMeasurement measurementFromType = (DiscreteMeasurement) measurements.getDiscreteMeasurements(DiscreteMeasurement.Type.TAP_POSITION).stream().findFirst().orElseThrow();
            assertEquals("Measurement_ID_1", measurementFromType.getId());
            assertEquals(DiscreteMeasurement.Type.TAP_POSITION, measurementFromType.getType());
            measurementFromType.setValue(false);
            measurementFromType.setValid(false);
            measurementFromType.removeProperty("other");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            TwoWindingsTransformer twoWindingsTransformer = network.getTwoWindingsTransformer("NHV2_NLOAD");
            DiscreteMeasurements measurements = twoWindingsTransformer.getExtension(DiscreteMeasurements.class);
            assertNotNull(measurements);

            DiscreteMeasurement measurement = measurements.getDiscreteMeasurement("Measurement_ID_1");
            assertNotNull(measurement);
            assertEquals(DiscreteMeasurement.Type.TAP_POSITION, measurement.getType());
            assertEquals(DiscreteMeasurement.ValueType.BOOLEAN, measurement.getValueType());
            assertFalse(measurement.getValueAsBoolean());
            assertFalse(measurement.isValid());
            assertNull(measurement.getProperty("other"));
            assertNotNull(measurement.getProperty("source"));
            measurement.remove();

            measurement = measurements.getDiscreteMeasurement("Measurement_ID_1");
            assertNull(measurement);
        }
    }

    @Test
    public void testBranchObservability() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = BatteryNetworkFactory.create(service.getNetworkFactory());
            Line line = network.getLine("NHV1_NHV2_1");
            line.newExtension(BranchObservabilityAdder.class)
                .withObservable(true)
                .withStandardDeviationP1(0.02d)
                .withStandardDeviationP2(0.04d)
                .withRedundantP1(true)
                .withRedundantP2(false)
                .withStandardDeviationQ1(0.5d)
                .withStandardDeviationQ2(1.0d)
                .withRedundantQ1(false)
                .withRedundantQ2(true)
                .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            Line line = network.getLine("NHV1_NHV2_1");

            BranchObservability<Line> branchObservability = line.getExtension(BranchObservability.class);
            assertEquals("branchObservability", branchObservability.getName());
            assertEquals("NHV1_NHV2_1", branchObservability.getExtendable().getId());

            assertTrue(branchObservability.isObservable());
            assertEquals(0.02d, branchObservability.getQualityP1().getStandardDeviation(), 0d);
            assertEquals(0.04d, branchObservability.getQualityP2().getStandardDeviation(), 0d);
            assertTrue(branchObservability.getQualityP1().isRedundant().isPresent());
            assertTrue(branchObservability.getQualityP1().isRedundant().get());
            assertTrue(branchObservability.getQualityP2().isRedundant().isPresent());
            assertFalse(branchObservability.getQualityP2().isRedundant().get());

            assertEquals(0.5d, branchObservability.getQualityQ1().getStandardDeviation(), 0d);
            assertEquals(1.0d, branchObservability.getQualityQ2().getStandardDeviation(), 0d);
            assertTrue(branchObservability.getQualityQ1().isRedundant().isPresent());
            assertFalse(branchObservability.getQualityQ1().isRedundant().get());
            assertTrue(branchObservability.getQualityQ2().isRedundant().isPresent());
            assertTrue(branchObservability.getQualityQ2().isRedundant().get());
        }
    }

    @Test
    public void testInjectionObservability() {
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = BatteryNetworkFactory.create(service.getNetworkFactory());
            Generator generator = network.getGenerator("GEN");
            generator.newExtension(InjectionObservabilityAdder.class)
                .withObservable(false)
                .withStandardDeviationP(0.02d)
                .withStandardDeviationQ(0.03d)
                .withStandardDeviationV(0.04d)
                .withRedundantP(true)
                .withRedundantQ(false)
                .withRedundantV(true)
                .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(service.getNetworkIds().keySet().iterator().next());
            Generator generator = network.getGenerator("GEN");

            InjectionObservability<Generator> generatorObservability = generator.getExtension(InjectionObservability.class);
            assertEquals("injectionObservability", generatorObservability.getName());
            assertEquals("GEN", generatorObservability.getExtendable().getId());

            assertFalse(generatorObservability.isObservable());
            assertEquals(0.02d, generatorObservability.getQualityP().getStandardDeviation(), 0d);
            assertTrue(generatorObservability.getQualityP().isRedundant().isPresent());
            assertTrue(generatorObservability.getQualityP().isRedundant().get());

            assertEquals(0.03d, generatorObservability.getQualityQ().getStandardDeviation(), 0d);
            assertTrue(generatorObservability.getQualityQ().isRedundant().isPresent());
            assertFalse(generatorObservability.getQualityQ().isRedundant().get());

            assertEquals(0.04d, generatorObservability.getQualityV().getStandardDeviation(), 0d);
            assertTrue(generatorObservability.getQualityV().isRedundant().isPresent());
            assertTrue(generatorObservability.getQualityV().isRedundant().get());
        }
    }
}
