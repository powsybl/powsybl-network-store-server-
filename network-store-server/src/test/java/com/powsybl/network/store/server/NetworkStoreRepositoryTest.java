/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.iidm.network.LimitType;
import com.powsybl.iidm.network.StaticVarCompensator;
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.LimitsInfos;
import com.powsybl.network.store.server.dto.OwnerInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class NetworkStoreRepositoryTest {

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @Autowired
    private NetworkStoreRepository networkStoreRepository;

    @Test
    void insertTemporaryLimitsInLinesTest() {

        String equipmentIdA = "idLineA";
        String equipmentIdB = "idLineB";

        OwnerInfo infoLineA = new OwnerInfo(
                equipmentIdA,
                ResourceType.LINE,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoLineB = new OwnerInfo(
                equipmentIdB,
                ResourceType.LINE,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoLineX = new OwnerInfo(
                "badID",
                ResourceType.LINE,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );

        Resource<LineAttributes> resLineA = Resource.lineBuilder()
                .id(equipmentIdA)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .name("idLineA")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                                .build()))
                        .build())
                .build();

        Resource<LineAttributes> resLineB = Resource.lineBuilder()
                .id(equipmentIdB)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .name("idLineB")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                                .build()))
                        .build())
                .build();

        assertEquals(resLineA.getId(), infoLineA.getEquipmentId());
        assertEquals(resLineB.getId(), infoLineB.getEquipmentId());
        assertNotEquals(resLineA.getId(), infoLineX.getEquipmentId());

        TemporaryLimitAttributes templimitAOkSide1a = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitAOkSide2a = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitAOkSide2b = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(200)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        // If there are multiple instance of a limit on the same side with the same acceptable duration, only one is kept.
        TemporaryLimitAttributes templimitAOkSide2bSameAcceptableDuration = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(200)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitWrongEquipmentId = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitBOkSide1a = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitBOkSide1b = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(200)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitBOkSide1c = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(300)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        List<Resource<LineAttributes>> lines = new ArrayList<>();
        lines.add(resLineA);
        lines.add(resLineB);

        List<TemporaryLimitAttributes> temporaryLimitsA = new ArrayList<>();
        temporaryLimitsA.add(templimitAOkSide1a);
        temporaryLimitsA.add(templimitAOkSide2a);
        temporaryLimitsA.add(templimitAOkSide2b);
        temporaryLimitsA.add(templimitAOkSide2bSameAcceptableDuration);

        List<TemporaryLimitAttributes> temporaryLimitsB = new ArrayList<>();
        temporaryLimitsB.add(templimitBOkSide1a);
        temporaryLimitsB.add(templimitBOkSide1b);
        temporaryLimitsB.add(templimitBOkSide1c);

        List<TemporaryLimitAttributes> temporaryLimitsX = new ArrayList<>();
        temporaryLimitsX.add(templimitWrongEquipmentId);

        Map<OwnerInfo, LimitsInfos> map = new HashMap<>();
        LimitsInfos limitsInfosA = new LimitsInfos();
        limitsInfosA.setTemporaryLimits(temporaryLimitsA);
        map.put(infoLineA, limitsInfosA);
        LimitsInfos limitsInfosB = new LimitsInfos();
        limitsInfosB.setTemporaryLimits(temporaryLimitsB);
        map.put(infoLineB, limitsInfosB);
        LimitsInfos limitsInfosX = new LimitsInfos();
        limitsInfosX.setTemporaryLimits(temporaryLimitsX);
        map.put(infoLineX, limitsInfosX);

        assertNull(resLineA.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNull(resLineA.getAttributes().getOperationalLimitsGroup2("group1"));
        assertNull(resLineB.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNull(resLineB.getAttributes().getOperationalLimitsGroup2("group1"));

        networkStoreRepository.insertLimitsInEquipments(NETWORK_UUID, lines, new HashMap<>());

        assertNull(resLineA.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNull(resLineA.getAttributes().getOperationalLimitsGroup2("group1"));
        assertNull(resLineB.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNull(resLineB.getAttributes().getOperationalLimitsGroup2("group1"));

        networkStoreRepository.insertLimitsInEquipments(NETWORK_UUID, lines, map);
        assertNotNull(resLineA.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNotNull(resLineA.getAttributes().getOperationalLimitsGroup2("group1").getCurrentLimits().getTemporaryLimits());
        assertEquals(1, resLineA.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits().size());
        assertEquals(2, resLineA.getAttributes().getOperationalLimitsGroup2("group1").getCurrentLimits().getTemporaryLimits().size());
        assertNotNull(resLineB.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits());
        assertNull(resLineB.getAttributes().getOperationalLimitsGroup2("group1"));
        assertEquals(3, resLineB.getAttributes().getOperationalLimitsGroup1("group1").getCurrentLimits().getTemporaryLimits().size());
    }

    @Test
    void insertReactiveCapabilityCurvesInGeneratorsTest() {

        String equipmentIdA = "idGeneratorA";
        String equipmentIdB = "idGeneratorB";
        String equipmentIdMinMax = "idGeneratorMinMax";

        OwnerInfo infoGeneratorA = new OwnerInfo(
                equipmentIdA,
                ResourceType.GENERATOR,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoGeneratorB = new OwnerInfo(
                equipmentIdB,
                ResourceType.GENERATOR,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoGeneratorMinMax = new OwnerInfo(
                equipmentIdMinMax,
                ResourceType.GENERATOR,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoGeneratorX = new OwnerInfo(
                "badID",
                ResourceType.GENERATOR,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );

        Resource<GeneratorAttributes> resGeneratorA = Resource.generatorBuilder()
                .id(equipmentIdA)
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("idGeneratorA")
                        .build()) // In this case, the reactivelimits are not initialized
                .build();

        Resource<GeneratorAttributes> resGeneratorB = Resource.generatorBuilder()
                .id(equipmentIdB)
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl2")
                        .name("idGeneratorB")
                        .reactiveLimits(ReactiveCapabilityCurveAttributes.builder().build()) // In this case, the reactivelimits are already initialized as ReactiveCapabilityCurveAttributes
                        .build())
                .build();

        Resource<GeneratorAttributes> resGeneratorMinMax = Resource.generatorBuilder()
                .id(equipmentIdMinMax)
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl3")
                        .name("idGeneratorMinMax")
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder() // In this case, the reactivelimits are already initialized as MinMaxReactiveLimitsAttributes
                                .maxQ(50.)
                                .minQ(20.)
                                .build())
                        .build())
                .build();

        assertEquals(resGeneratorA.getId(), infoGeneratorA.getEquipmentId());
        assertEquals(resGeneratorB.getId(), infoGeneratorB.getEquipmentId());
        assertEquals(resGeneratorMinMax.getId(), infoGeneratorMinMax.getEquipmentId());
        assertNotEquals(resGeneratorA.getId(), infoGeneratorX.getEquipmentId());
        assertNotEquals(resGeneratorB.getId(), infoGeneratorX.getEquipmentId());
        assertNotEquals(resGeneratorMinMax.getId(), infoGeneratorX.getEquipmentId());

        ReactiveCapabilityCurvePointAttributes curvePointOka = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(-100.)
                .maxQ(100.)
                .p(0.)
                .build();

        ReactiveCapabilityCurvePointAttributes curvePointOkb = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(10.)
                .maxQ(30.)
                .p(20.)
                .build();

        ReactiveCapabilityCurvePointAttributes curvePointOkc = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(5.)
                .maxQ(25.)
                .p(15.)
                .build();

        // If there are multiple instance of a curve point with the same value P, only one is kept.
        ReactiveCapabilityCurvePointAttributes curvePointSameValueP = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(10.)
                .maxQ(30.)
                .p(20.)
                .build();

        ReactiveCapabilityCurvePointAttributes curvePointWrongEquipmentId = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(10.)
                .maxQ(30.)
                .p(20.)
                .build();

        List<Resource<GeneratorAttributes>> generators = new ArrayList<>();
        generators.add(resGeneratorA);
        generators.add(resGeneratorB);
        generators.add(resGeneratorMinMax);

        List<ReactiveCapabilityCurvePointAttributes> curvePointsForGeneratorA = new ArrayList<>();
        curvePointsForGeneratorA.add(curvePointOka);
        curvePointsForGeneratorA.add(curvePointOkb);
        curvePointsForGeneratorA.add(curvePointOkc);
        curvePointsForGeneratorA.add(curvePointSameValueP);

        List<ReactiveCapabilityCurvePointAttributes> curvePointsForGeneratorB = new ArrayList<>();
        curvePointsForGeneratorB.add(curvePointOka);
        curvePointsForGeneratorB.add(curvePointOkb);

        List<ReactiveCapabilityCurvePointAttributes> curvePointsX = new ArrayList<>();
        curvePointsX.add(curvePointWrongEquipmentId);

        Map<OwnerInfo, List<ReactiveCapabilityCurvePointAttributes>> map = new HashMap<>();

        map.put(infoGeneratorA, curvePointsForGeneratorA);
        map.put(infoGeneratorB, curvePointsForGeneratorB);
        map.put(infoGeneratorX, curvePointsX);

        assertNull(resGeneratorA.getAttributes().getReactiveLimits());
        assertInstanceOf(ReactiveCapabilityCurveAttributes.class, resGeneratorB.getAttributes().getReactiveLimits());
        assertNull(((ReactiveCapabilityCurveAttributes) resGeneratorB.getAttributes().getReactiveLimits()).getPoints());
        assertInstanceOf(MinMaxReactiveLimitsAttributes.class, resGeneratorMinMax.getAttributes().getReactiveLimits());

        networkStoreRepository.insertReactiveCapabilityCurvePointsInEquipments(NETWORK_UUID, generators, new HashMap<>());

        assertNull(resGeneratorA.getAttributes().getReactiveLimits());
        assertInstanceOf(ReactiveCapabilityCurveAttributes.class, resGeneratorB.getAttributes().getReactiveLimits());
        assertNull(((ReactiveCapabilityCurveAttributes) resGeneratorB.getAttributes().getReactiveLimits()).getPoints());
        assertInstanceOf(MinMaxReactiveLimitsAttributes.class, resGeneratorMinMax.getAttributes().getReactiveLimits());

        networkStoreRepository.insertReactiveCapabilityCurvePointsInEquipments(NETWORK_UUID, generators, map);

        assertInstanceOf(ReactiveCapabilityCurveAttributes.class, resGeneratorA.getAttributes().getReactiveLimits());
        assertNotNull(((ReactiveCapabilityCurveAttributes) resGeneratorA.getAttributes().getReactiveLimits()).getPoints());
        assertEquals(3, ((ReactiveCapabilityCurveAttributes) resGeneratorA.getAttributes().getReactiveLimits()).getPoints().size());

        assertInstanceOf(ReactiveCapabilityCurveAttributes.class, resGeneratorB.getAttributes().getReactiveLimits());
        assertNotNull(((ReactiveCapabilityCurveAttributes) resGeneratorB.getAttributes().getReactiveLimits()).getPoints());
        assertEquals(2, ((ReactiveCapabilityCurveAttributes) resGeneratorB.getAttributes().getReactiveLimits()).getPoints().size());

        assertInstanceOf(MinMaxReactiveLimitsAttributes.class, resGeneratorMinMax.getAttributes().getReactiveLimits());
    }

    @Test
    void insertTapChangerStepsInTwoWindingsTranformerTest() {

        String equipmentIdA = "id2WTransformerA";
        String equipmentIdB = "id2WTransformerB";

        OwnerInfo info2WTransformerA = new OwnerInfo(
                equipmentIdA,
                ResourceType.TWO_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo info2WTransformerB = new OwnerInfo(
                equipmentIdB,
                ResourceType.TWO_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo info2WTransformerX = new OwnerInfo(
                "badID",
                ResourceType.TWO_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );

        Resource<TwoWindingsTransformerAttributes> res2WTransformerA = Resource.twoWindingsTransformerBuilder()
                .id(equipmentIdA)
                .attributes(TwoWindingsTransformerAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .name("id2WTransformerA")
                        .ratioTapChangerAttributes(RatioTapChangerAttributes.builder()
                                .lowTapPosition(20)
                                .build())
                        .build())
                .build();

        Resource<TwoWindingsTransformerAttributes> res2WTransformerB = Resource.twoWindingsTransformerBuilder()
                .id(equipmentIdB)
                .attributes(TwoWindingsTransformerAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .name("id2WTransformerB")
                        .phaseTapChangerAttributes(PhaseTapChangerAttributes.builder()
                                .lowTapPosition(30)
                                .build())
                        .build())
                .build();

        assertEquals(res2WTransformerA.getId(), info2WTransformerA.getEquipmentId());
        assertEquals(res2WTransformerB.getId(), info2WTransformerB.getEquipmentId());
        assertNotEquals(res2WTransformerA.getId(), info2WTransformerX.getEquipmentId());

        TapChangerStepAttributes ratioStepA1 = TapChangerStepAttributes.builder()
                .rho(1.)
                .r(1.)
                .g(1.)
                .b(1.)
                .x(1.)
                .side(0)
                .index(0)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepA2 = TapChangerStepAttributes.builder()
                .rho(2.)
                .r(2.)
                .g(2.)
                .b(2.)
                .x(2.)
                .side(0)
                .index(1)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepA3 = TapChangerStepAttributes.builder()
                .rho(3.)
                .r(3.)
                .g(3.)
                .b(3.)
                .x(3.)
                .side(0)
                .index(2)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes phaseStepB1 = TapChangerStepAttributes.builder()
                .rho(10.)
                .r(10.)
                .g(10.)
                .b(10.)
                .x(10.)
                .alpha(10.)
                .side(0)
                .index(0)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepB2 = TapChangerStepAttributes.builder()
                .rho(20.)
                .r(20.)
                .g(20.)
                .b(20.)
                .x(20.)
                .alpha(20.)
                .side(0)
                .index(1)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepB3 = TapChangerStepAttributes.builder()
                .rho(30.)
                .r(30.)
                .g(30.)
                .b(30.)
                .x(30.)
                .alpha(30.)
                .side(0)
                .index(2)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepB4 = TapChangerStepAttributes.builder()
                .rho(40.)
                .r(40.)
                .g(40.)
                .b(40.)
                .x(40.)
                .alpha(40.)
                .side(0)
                .index(3)
                .type(TapChangerType.PHASE)
                .build();

        assertEquals(TapChangerType.RATIO, ratioStepA1.getType());
        assertEquals(TapChangerType.PHASE, phaseStepB1.getType());

        List<Resource<TwoWindingsTransformerAttributes>> twoWTransformers = new ArrayList<>();
        twoWTransformers.add(res2WTransformerA);
        twoWTransformers.add(res2WTransformerB);

        List<TapChangerStepAttributes> tapChangerStepsA = new ArrayList<>();
        tapChangerStepsA.add(ratioStepA1);
        tapChangerStepsA.add(ratioStepA2);
        tapChangerStepsA.add(ratioStepA3);

        List<TapChangerStepAttributes> tapChangerStepsB = new ArrayList<>();
        tapChangerStepsB.add(phaseStepB1);
        tapChangerStepsB.add(phaseStepB2);
        tapChangerStepsB.add(phaseStepB3);
        tapChangerStepsB.add(phaseStepB4);

        Map<OwnerInfo, List<TapChangerStepAttributes>> mapA = new HashMap<>();
        Map<OwnerInfo, List<TapChangerStepAttributes>> mapB = new HashMap<>();

        mapA.put(info2WTransformerA, tapChangerStepsA);
        mapB.put(info2WTransformerB, tapChangerStepsB);

        assertNull(res2WTransformerA.getAttributes().getRatioTapChangerAttributes().getSteps());
        assertNull(res2WTransformerA.getAttributes().getPhaseTapChangerAttributes());
        assertNull(res2WTransformerB.getAttributes().getRatioTapChangerAttributes());
        assertNull(res2WTransformerB.getAttributes().getPhaseTapChangerAttributes().getSteps());

        networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, twoWTransformers, new HashMap<>());

        assertNull(res2WTransformerA.getAttributes().getRatioTapChangerAttributes().getSteps());
        assertNull(res2WTransformerA.getAttributes().getPhaseTapChangerAttributes());
        assertNull(res2WTransformerB.getAttributes().getRatioTapChangerAttributes());
        assertNull(res2WTransformerB.getAttributes().getPhaseTapChangerAttributes().getSteps());

        networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, twoWTransformers, mapA);
        assertNotNull(res2WTransformerA.getAttributes().getRatioTapChangerAttributes().getSteps());
        assertNull(res2WTransformerA.getAttributes().getPhaseTapChangerAttributes());
        assertEquals(3, res2WTransformerA.getAttributes().getRatioTapChangerAttributes().getSteps().size());

        networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, twoWTransformers, mapB);
        assertNotNull(res2WTransformerB.getAttributes().getPhaseTapChangerAttributes().getSteps());
        assertNull(res2WTransformerB.getAttributes().getRatioTapChangerAttributes());
        assertEquals(4, res2WTransformerB.getAttributes().getPhaseTapChangerAttributes().getSteps().size());

    }

    @Test
    void insertTapChangerStepsInThreeWindingsTranformerTest() {

        String equipmentIdA = "id3WTransformerA";
        String equipmentIdB = "id3WTransformerB";

        OwnerInfo info3WTransformerA = new OwnerInfo(
                equipmentIdA,
                ResourceType.THREE_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo info3WTransformerB = new OwnerInfo(
                equipmentIdB,
                ResourceType.THREE_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo info3WTransformerX = new OwnerInfo(
                "badID",
                ResourceType.TWO_WINDINGS_TRANSFORMER,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );

        Resource<ThreeWindingsTransformerAttributes> res3WTransformerA = Resource.threeWindingsTransformerBuilder()
                .id(equipmentIdA)
                .attributes(ThreeWindingsTransformerAttributes.builder()
                        .name("id3WTransformerA")
                        .ratedU0(1)
                        .leg1(LegAttributes.builder()
                                .ratioTapChangerAttributes(RatioTapChangerAttributes.builder()
                                .lowTapPosition(20)
                                .build())
                        .build()
                        )
                        .leg2(new LegAttributes())
                        .leg3(new LegAttributes())
                        .build())
                .build();

        Resource<ThreeWindingsTransformerAttributes> res3WTransformerB = Resource.threeWindingsTransformerBuilder()
                .id(equipmentIdB)
                .attributes(ThreeWindingsTransformerAttributes.builder()
                        .name("id3WTransformerB")
                        .ratedU0(1)
                        .leg1(new LegAttributes())
                        .leg2(LegAttributes.builder()
                                .phaseTapChangerAttributes(PhaseTapChangerAttributes.builder()
                                .lowTapPosition(20)
                                .build())
                        .build()
                        )
                        .leg3(new LegAttributes())
                        .build())
                .build();

        assertEquals(res3WTransformerA.getId(), info3WTransformerA.getEquipmentId());
        assertEquals(res3WTransformerB.getId(), info3WTransformerB.getEquipmentId());
        assertNotEquals(res3WTransformerA.getId(), info3WTransformerX.getEquipmentId());

        TapChangerStepAttributes ratioStepAS11 = TapChangerStepAttributes.builder()
                .rho(1.)
                .r(1.)
                .g(1.)
                .b(1.)
                .x(1.)
                .side(1)
                .index(0)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepAS12 = TapChangerStepAttributes.builder()
                .rho(2.)
                .r(2.)
                .g(2.)
                .b(2.)
                .x(2.)
                .side(1)
                .index(1)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepAS21 = TapChangerStepAttributes.builder()
                .rho(1.)
                .r(1.)
                .g(1.)
                .b(1.)
                .x(1.)
                .side(2)
                .index(0)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepAS22 = TapChangerStepAttributes.builder()
                .rho(2.)
                .r(2.)
                .g(2.)
                .b(2.)
                .x(2.)
                .side(2)
                .index(1)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes ratioStepABad = TapChangerStepAttributes.builder()
                .rho(3.)
                .r(3.)
                .g(3.)
                .b(3.)
                .x(3.)
                .side(4) // this side doesn't exists
                .index(2)
                .type(TapChangerType.RATIO)
                .build();

        TapChangerStepAttributes phaseStepBS21 = TapChangerStepAttributes.builder()
                .rho(10.)
                .r(10.)
                .g(10.)
                .b(10.)
                .x(10.)
                .alpha(10.)
                .side(2)
                .index(0)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepBS22 = TapChangerStepAttributes.builder()
                .rho(20.)
                .r(20.)
                .g(20.)
                .b(20.)
                .x(20.)
                .alpha(20.)
                .side(2)
                .index(1)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepBS31 = TapChangerStepAttributes.builder()
                .rho(30.)
                .r(30.)
                .g(30.)
                .b(30.)
                .x(30.)
                .alpha(30.)
                .side(3)
                .index(0)
                .type(TapChangerType.PHASE)
                .build();

        TapChangerStepAttributes phaseStepBBad = TapChangerStepAttributes.builder()
                .rho(40.)
                .r(40.)
                .g(40.)
                .b(40.)
                .x(40.)
                .alpha(40.)
                .side(4) // this side doesn't exists
                .index(0)
                .type(TapChangerType.PHASE)
                .build();

        List<Resource<ThreeWindingsTransformerAttributes>> threeWTransformers = new ArrayList<>();
        threeWTransformers.add(res3WTransformerA);
        threeWTransformers.add(res3WTransformerB);

        List<TapChangerStepAttributes> tapChangerStepsA = new ArrayList<>();
        tapChangerStepsA.add(ratioStepAS11);
        tapChangerStepsA.add(ratioStepAS12);
        tapChangerStepsA.add(ratioStepAS21);
        tapChangerStepsA.add(ratioStepAS22);
        tapChangerStepsA.add(ratioStepABad);

        List<TapChangerStepAttributes> tapChangerStepsB = new ArrayList<>();
        tapChangerStepsB.add(phaseStepBS21);
        tapChangerStepsB.add(phaseStepBS22);
        tapChangerStepsB.add(phaseStepBS31);
        tapChangerStepsB.add(phaseStepBBad);

        Map<OwnerInfo, List<TapChangerStepAttributes>> mapA = new HashMap<>();
        Map<OwnerInfo, List<TapChangerStepAttributes>> mapB = new HashMap<>();

        mapA.put(info3WTransformerA, tapChangerStepsA);
        mapB.put(info3WTransformerB, tapChangerStepsB);

        assertNull(res3WTransformerA.getAttributes().getLeg(1).getRatioTapChangerAttributes().getSteps());
        assertNull(res3WTransformerA.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(2).getRatioTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(2).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(3).getRatioTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(3).getPhaseTapChangerAttributes());

        assertNull(res3WTransformerB.getAttributes().getLeg(1).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(2).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(2).getPhaseTapChangerAttributes().getSteps());
        assertNull(res3WTransformerB.getAttributes().getLeg(3).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(3).getPhaseTapChangerAttributes());

        networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, threeWTransformers, new HashMap<>());

        assertNull(res3WTransformerA.getAttributes().getLeg(1).getRatioTapChangerAttributes().getSteps());
        assertNull(res3WTransformerA.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(2).getRatioTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(2).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(3).getRatioTapChangerAttributes());
        assertNull(res3WTransformerA.getAttributes().getLeg(3).getPhaseTapChangerAttributes());

        assertNull(res3WTransformerB.getAttributes().getLeg(1).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(2).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(2).getPhaseTapChangerAttributes().getSteps());
        assertNull(res3WTransformerB.getAttributes().getLeg(3).getRatioTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(3).getPhaseTapChangerAttributes());

        // in A
        assertThrows(IllegalArgumentException.class, () ->
            networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, threeWTransformers, mapA));
        assertNotNull(res3WTransformerA.getAttributes().getLeg(1).getRatioTapChangerAttributes().getSteps());
        assertNull(res3WTransformerA.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertEquals(2, res3WTransformerA.getAttributes().getLeg(1).getRatioTapChangerAttributes().getSteps().size());

        assertNotNull(res3WTransformerA.getAttributes().getLeg(2).getRatioTapChangerAttributes().getSteps());
        assertNull(res3WTransformerA.getAttributes().getLeg(2).getPhaseTapChangerAttributes());
        assertEquals(2, res3WTransformerA.getAttributes().getLeg(2).getRatioTapChangerAttributes().getSteps().size());

        // in B
        assertThrows(IllegalArgumentException.class, () ->
            networkStoreRepository.insertTapChangerStepsInEquipments(NETWORK_UUID, threeWTransformers, mapB));
        assertNull(res3WTransformerB.getAttributes().getLeg(1).getPhaseTapChangerAttributes());
        assertNull(res3WTransformerB.getAttributes().getLeg(1).getRatioTapChangerAttributes());

        assertNotNull(res3WTransformerB.getAttributes().getLeg(2).getPhaseTapChangerAttributes().getSteps());
        assertNull(res3WTransformerB.getAttributes().getLeg(2).getRatioTapChangerAttributes());
        assertEquals(2, res3WTransformerB.getAttributes().getLeg(2).getPhaseTapChangerAttributes().getSteps().size());

        assertNotNull(res3WTransformerB.getAttributes().getLeg(3).getPhaseTapChangerAttributes().getSteps());
        assertNull(res3WTransformerB.getAttributes().getLeg(3).getRatioTapChangerAttributes());
        assertEquals(1, res3WTransformerB.getAttributes().getLeg(3).getPhaseTapChangerAttributes().getSteps().size());
    }

    @Test
    void test() {
        String loadId = "load1";
        String lineId = "line1";
        Resource<LineAttributes> line1 = Resource.lineBuilder()
                .id(lineId)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .build())
                .build();
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
                .id(loadId)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .build())
                .build();
        networkStoreRepository.createLines(NETWORK_UUID, List.of(line1));
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));
        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM);
        assertEquals(List.of(loadId, lineId), identifiablesIds);

        networkStoreRepository.deleteLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        networkStoreRepository.deleteLine(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, lineId);
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId).isEmpty());
        assertTrue(networkStoreRepository.getLine(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, lineId).isEmpty());
    }

    @Test
    void testRegulatingPointForGenerator() {
        String generatorId = "gen1";
        Resource<GeneratorAttributes> gen = Resource.generatorBuilder()
            .id(generatorId)
            .attributes(GeneratorAttributes.builder()
                    .voltageLevelId("vl1")
                    .name(generatorId)
                    .regulatingPoint(RegulatingPointAttributes.builder()
                        .localTerminal(TerminalRefAttributes.builder().connectableId(generatorId).build())
                        .regulatedResourceType(ResourceType.GENERATOR)
                        .regulatingEquipmentId(generatorId)
                        .regulatingTerminal(TerminalRefAttributes.builder().connectableId(generatorId).build())
                        .build())
                    .build())
            .build();
        networkStoreRepository.createGenerators(NETWORK_UUID, List.of(gen));
        String loadId = "load1";
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
            .id(loadId)
            .attributes(LoadAttributes.builder()
                .voltageLevelId("vl1")
                .build())
            .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));

        Optional<Resource<GeneratorAttributes>> generator = networkStoreRepository.getGenerator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, generatorId);
        assertTrue(generator.isPresent());
        assertEquals(generatorId, generator.get().getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertNull(generator.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(generator.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(generatorId, generator.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(generator.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertEquals(1, generator.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(generator.get().getAttributes().getRegulatingEquipments().containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, generator.get().getAttributes().getRegulatingEquipments().get(generatorId));

        // get vl generator
        List<Resource<GeneratorAttributes>> generatorList = networkStoreRepository.getVoltageLevelGenerators(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, "vl1");
        assertEquals(1, generatorList.size());
        Resource<GeneratorAttributes> generatorVl = generatorList.get(0);
        assertEquals(generatorId, generatorVl.getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertEquals(generatorId, generatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(generatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(generatorVl.getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(generatorId, generatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(generatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getSide());

        // update
        Resource<GeneratorAttributes> updatedGen = Resource.generatorBuilder()
            .id(generatorId)
            .variantNum(Resource.INITIAL_VARIANT_NUM)
            .attributes(GeneratorAttributes.builder()
                .voltageLevelId("vl1")
                .name(generatorId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(loadId).build())
                    .regulatedResourceType(ResourceType.LOAD)
                    .build())
                .build())
            .build();
        networkStoreRepository.updateGenerators(NETWORK_UUID, List.of(updatedGen));

        Optional<Resource<GeneratorAttributes>> generatorResult = networkStoreRepository.getGenerator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, generatorId);
        assertTrue(generatorResult.isPresent());
        assertEquals(loadId, generatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(generatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(generatorResult.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(generatorId, generatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(generatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertTrue(generatorResult.get().getAttributes().getRegulatingEquipments().isEmpty());

        Optional<Resource<LoadAttributes>> loadResult = networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(loadResult.isPresent());
        assertEquals(1, loadResult.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(loadResult.get().getAttributes().getRegulatingEquipments().containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, loadResult.get().getAttributes().getRegulatingEquipments().get(generatorId));

        // delete
        networkStoreRepository.deleteGenerator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, generatorId);
        networkStoreRepository.deleteLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId).isEmpty());
        assertTrue(networkStoreRepository.getGenerator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, generatorId).isEmpty());
    }

    @Test
    void testRegulatingPointForShuntCompensator() {
        String shuntCompensatorId = "shunt1";
        Resource<ShuntCompensatorAttributes> shunt = Resource.shuntCompensatorBuilder()
            .id(shuntCompensatorId)
            .attributes(ShuntCompensatorAttributes.builder()
                .voltageLevelId("vl1")
                .name(shuntCompensatorId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .localTerminal(TerminalRefAttributes.builder().connectableId(shuntCompensatorId).build())
                    .regulatedResourceType(ResourceType.SHUNT_COMPENSATOR)
                    .regulatingEquipmentId(shuntCompensatorId)
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(shuntCompensatorId).build())
                    .build())
                .build())
            .build();
        networkStoreRepository.createShuntCompensators(NETWORK_UUID, List.of(shunt));
        String loadId = "load1";
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
            .id(loadId)
            .attributes(LoadAttributes.builder()
                .voltageLevelId("vl1")
                .build())
            .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));

        Optional<Resource<ShuntCompensatorAttributes>> shuntCompensator = networkStoreRepository.getShuntCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, shuntCompensatorId);
        assertTrue(shuntCompensator.isPresent());
        assertEquals(shuntCompensatorId, shuntCompensator.get().getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertNull(shuntCompensator.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(shuntCompensator.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(shuntCompensatorId, shuntCompensator.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(shuntCompensator.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertEquals(1, shuntCompensator.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(shuntCompensator.get().getAttributes().getRegulatingEquipments().containsKey(shuntCompensatorId));
        assertEquals(ResourceType.SHUNT_COMPENSATOR, shuntCompensator.get().getAttributes().getRegulatingEquipments().get(shuntCompensatorId));

        // get vl shunt
        List<Resource<ShuntCompensatorAttributes>> shuntCompensatorList = networkStoreRepository.getVoltageLevelShuntCompensators(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, "vl1");
        assertEquals(1, shuntCompensatorList.size());
        Resource<ShuntCompensatorAttributes> shuntCompensatorVl = shuntCompensatorList.get(0);
        assertEquals(shuntCompensatorId, shuntCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertEquals(shuntCompensatorId, shuntCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(shuntCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(shuntCompensatorVl.getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(shuntCompensatorId, shuntCompensatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(shuntCompensatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getSide());

        // update
        Resource<ShuntCompensatorAttributes> updatedGen = Resource.shuntCompensatorBuilder()
            .id(shuntCompensatorId)
            .variantNum(Resource.INITIAL_VARIANT_NUM)
            .attributes(ShuntCompensatorAttributes.builder()
                .voltageLevelId("vl1")
                .name(shuntCompensatorId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(loadId).build())
                    .regulatedResourceType(ResourceType.LOAD)
                    .build())
                .build())
            .build();
        networkStoreRepository.updateShuntCompensators(NETWORK_UUID, List.of(updatedGen));

        Optional<Resource<ShuntCompensatorAttributes>> shuntCompensatorResult = networkStoreRepository.getShuntCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, shuntCompensatorId);
        assertTrue(shuntCompensatorResult.isPresent());
        assertEquals(loadId, shuntCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(shuntCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(shuntCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(shuntCompensatorId, shuntCompensatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(shuntCompensatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertTrue(shuntCompensatorResult.get().getAttributes().getRegulatingEquipments().isEmpty());

        Optional<Resource<LoadAttributes>> loadResult = networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(loadResult.isPresent());
        assertEquals(1, loadResult.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(loadResult.get().getAttributes().getRegulatingEquipments().containsKey(shuntCompensatorId));
        assertEquals(ResourceType.SHUNT_COMPENSATOR, loadResult.get().getAttributes().getRegulatingEquipments().get(shuntCompensatorId));

        // delete
        networkStoreRepository.deleteShuntCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, shuntCompensatorId);
        networkStoreRepository.deleteLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId).isEmpty());
        assertTrue(networkStoreRepository.getShuntCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, shuntCompensatorId).isEmpty());
    }

    @Test
    void testRegulatingPointForStaticVarCompensator() {
        String staticVarCompensatorId = "svc1";
        Resource<StaticVarCompensatorAttributes> staticVarCompensator = Resource.staticVarCompensatorBuilder()
            .id(staticVarCompensatorId)
            .attributes(StaticVarCompensatorAttributes.builder()
                .voltageLevelId("vl1")
                .name(staticVarCompensatorId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .localTerminal(TerminalRefAttributes.builder().connectableId(staticVarCompensatorId).build())
                    .regulatedResourceType(ResourceType.STATIC_VAR_COMPENSATOR)
                    .regulationMode(StaticVarCompensator.RegulationMode.VOLTAGE.toString())
                    .regulatingEquipmentId(staticVarCompensatorId)
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(staticVarCompensatorId).build())
                    .build())
                .build())
            .build();
        networkStoreRepository.createStaticVarCompensators(NETWORK_UUID, List.of(staticVarCompensator));
        String loadId = "load1";
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
            .id(loadId)
            .attributes(LoadAttributes.builder()
                .voltageLevelId("vl1")
                .build())
            .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));

        Optional<Resource<StaticVarCompensatorAttributes>> staticVarCompensatorCreation = networkStoreRepository.getStaticVarCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, staticVarCompensatorId);
        assertTrue(staticVarCompensatorCreation.isPresent());
        assertEquals(staticVarCompensatorId, staticVarCompensatorCreation.get().getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertNull(staticVarCompensatorCreation.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertEquals(StaticVarCompensator.RegulationMode.VOLTAGE.toString(), staticVarCompensatorCreation.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(staticVarCompensatorId, staticVarCompensatorCreation.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(staticVarCompensatorCreation.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertEquals(1, staticVarCompensatorCreation.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(staticVarCompensatorCreation.get().getAttributes().getRegulatingEquipments().containsKey(staticVarCompensatorId));
        assertEquals(ResourceType.STATIC_VAR_COMPENSATOR, staticVarCompensatorCreation.get().getAttributes().getRegulatingEquipments().get(staticVarCompensatorId));

        // get vl svc
        List<Resource<StaticVarCompensatorAttributes>> staticVarCompensatorList = networkStoreRepository.getVoltageLevelStaticVarCompensators(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, "vl1");
        assertEquals(1, staticVarCompensatorList.size());
        Resource<StaticVarCompensatorAttributes> staticVarCompensatorVl = staticVarCompensatorList.get(0);
        assertEquals(staticVarCompensatorId, staticVarCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertEquals(staticVarCompensatorId, staticVarCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(staticVarCompensatorVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertEquals(StaticVarCompensator.RegulationMode.VOLTAGE.toString(), staticVarCompensatorVl.getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(staticVarCompensatorId, staticVarCompensatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(staticVarCompensatorVl.getAttributes().getRegulatingPoint().getLocalTerminal().getSide());

        // update
        Resource<StaticVarCompensatorAttributes> updatedGen = Resource.staticVarCompensatorBuilder()
            .id(staticVarCompensatorId)
            .variantNum(Resource.INITIAL_VARIANT_NUM)
            .attributes(StaticVarCompensatorAttributes.builder()
                .voltageLevelId("vl1")
                .name(staticVarCompensatorId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(loadId).build())
                    .regulatedResourceType(ResourceType.LOAD)
                    .regulationMode(StaticVarCompensator.RegulationMode.REACTIVE_POWER.toString())
                    .build())
                .build())
            .build();
        networkStoreRepository.updateStaticVarCompensators(NETWORK_UUID, List.of(updatedGen));

        Optional<Resource<StaticVarCompensatorAttributes>> staticVarCompensatorResult = networkStoreRepository.getStaticVarCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, staticVarCompensatorId);
        assertTrue(staticVarCompensatorResult.isPresent());
        assertEquals(loadId, staticVarCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(staticVarCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertEquals(StaticVarCompensator.RegulationMode.REACTIVE_POWER.toString(), staticVarCompensatorResult.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(staticVarCompensatorId, staticVarCompensatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(staticVarCompensatorResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertTrue(staticVarCompensatorResult.get().getAttributes().getRegulatingEquipments().isEmpty());

        Optional<Resource<LoadAttributes>> loadResult = networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(loadResult.isPresent());
        assertEquals(1, loadResult.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(loadResult.get().getAttributes().getRegulatingEquipments().containsKey(staticVarCompensatorId));
        assertEquals(ResourceType.STATIC_VAR_COMPENSATOR, loadResult.get().getAttributes().getRegulatingEquipments().get(staticVarCompensatorId));

        // delete
        networkStoreRepository.deleteStaticVarCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, staticVarCompensatorId);
        networkStoreRepository.deleteLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId).isEmpty());
        assertTrue(networkStoreRepository.getStaticVarCompensator(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, staticVarCompensatorId).isEmpty());
    }

    @Test
    void testRegulatingPointForVSC() {
        String vscId = "vsc1";
        Resource<VscConverterStationAttributes> staticVarCompensator = Resource.vscConverterStationBuilder()
            .id(vscId)
            .attributes(VscConverterStationAttributes.builder()
                .voltageLevelId("vl1")
                .name(vscId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .localTerminal(TerminalRefAttributes.builder().connectableId(vscId).build())
                    .regulatedResourceType(ResourceType.VSC_CONVERTER_STATION)
                    .regulatingEquipmentId(vscId)
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(vscId).build())
                    .build())
                .build())
            .build();
        networkStoreRepository.createVscConverterStations(NETWORK_UUID, List.of(staticVarCompensator));
        String loadId = "load1";
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
            .id(loadId)
            .attributes(LoadAttributes.builder()
                .voltageLevelId("vl1")
                .build())
            .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));

        Optional<Resource<VscConverterStationAttributes>> vscCreation = networkStoreRepository.getVscConverterStation(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, vscId);
        assertTrue(vscCreation.isPresent());
        assertEquals(vscId, vscCreation.get().getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertNull(vscCreation.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(vscCreation.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(vscId, vscCreation.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(vscCreation.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertEquals(1, vscCreation.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(vscCreation.get().getAttributes().getRegulatingEquipments().containsKey(vscId));
        assertEquals(ResourceType.VSC_CONVERTER_STATION, vscCreation.get().getAttributes().getRegulatingEquipments().get(vscId));

        // get vl svc
        List<Resource<VscConverterStationAttributes>> vscList = networkStoreRepository.getVoltageLevelVscConverterStations(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, "vl1");
        assertEquals(1, vscList.size());
        Resource<VscConverterStationAttributes> vscVl = vscList.get(0);
        assertEquals(vscId, vscVl.getAttributes().getRegulatingPoint().getRegulatingEquipmentId());
        assertEquals(vscId, vscVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(vscVl.getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(vscVl.getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(vscId, vscVl.getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(vscVl.getAttributes().getRegulatingPoint().getLocalTerminal().getSide());

        // update
        Resource<VscConverterStationAttributes> updatedGen = Resource.vscConverterStationBuilder()
            .id(vscId)
            .variantNum(Resource.INITIAL_VARIANT_NUM)
            .attributes(VscConverterStationAttributes.builder()
                .voltageLevelId("vl1")
                .name(vscId)
                .regulatingPoint(RegulatingPointAttributes.builder()
                    .regulatingTerminal(TerminalRefAttributes.builder().connectableId(loadId).build())
                    .regulatedResourceType(ResourceType.LOAD)
                    .build())
                .build())
            .build();
        networkStoreRepository.updateVscConverterStations(NETWORK_UUID, List.of(updatedGen));

        Optional<Resource<VscConverterStationAttributes>> vscResult = networkStoreRepository.getVscConverterStation(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, vscId);
        assertTrue(vscResult.isPresent());
        assertEquals(loadId, vscResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getConnectableId());
        assertNull(vscResult.get().getAttributes().getRegulatingPoint().getRegulatingTerminal().getSide());
        assertNull(vscResult.get().getAttributes().getRegulatingPoint().getRegulationMode());
        assertEquals(vscId, vscResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getConnectableId());
        assertNull(vscResult.get().getAttributes().getRegulatingPoint().getLocalTerminal().getSide());
        assertTrue(vscResult.get().getAttributes().getRegulatingEquipments().isEmpty());

        Optional<Resource<LoadAttributes>> loadResult = networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(loadResult.isPresent());
        assertEquals(1, loadResult.get().getAttributes().getRegulatingEquipments().size());
        assertTrue(loadResult.get().getAttributes().getRegulatingEquipments().containsKey(vscId));
        assertEquals(ResourceType.VSC_CONVERTER_STATION, loadResult.get().getAttributes().getRegulatingEquipments().get(vscId));

        // delete
        networkStoreRepository.deleteVscConverterStation(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, vscId);
        networkStoreRepository.deleteLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId);
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, loadId).isEmpty());
        assertTrue(networkStoreRepository.getVscConverterStation(NETWORK_UUID, Resource.INITIAL_VARIANT_NUM, vscId).isEmpty());
    }
}
