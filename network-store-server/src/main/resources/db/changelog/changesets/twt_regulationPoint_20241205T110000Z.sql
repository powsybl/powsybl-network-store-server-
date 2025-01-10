-- two windings transformer
insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'TWO_WINDINGS_TRANSFORMER','PHASE_TAP_CHANGER', phasetapchangerregulationmode, null, null, phasetapchangerterminalrefconnectableid, phasetapchangerterminalrefside, phasetapchangerregulating
from twowindingstransformer
where phasetapchangerregulating is not null;
insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'TWO_WINDINGS_TRANSFORMER', 'RATIO_TAP_CHANGER', ratiotapchangerregulationmode, null, null, ratiotapchangerterminalrefconnectableid, ratiotapchangerterminalrefside, ratiotapchangerregulating
from twowindingstransformer
where ratiotapchangerregulating is not null;

-- three windings transformer
-- side 1
insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER','PHASE_TAP_CHANGER_SIDE_ONE', phasetapchangerregulationmode1, null, null, phasetapchangerterminalrefconnectableid1, phasetapchangerterminalrefside1, phasetapchangerregulating1
from threewindingstransformer
where phasetapchangerregulating1 is not null;

insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'RATIO_TAP_CHANGER_SIDE_ONE', ratiotapchangerregulationmode1, null, null, ratiotapchangerterminalrefconnectableid1, ratiotapchangerterminalrefside1, ratiotapchangerregulating1
from threewindingstransformer
where ratiotapchangerregulating1 is not null;
-- side 2
insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'PHASE_TAP_CHANGER_SIDE_TWO', phasetapchangerregulationmode2, null, null, phasetapchangerterminalrefconnectableid2, phasetapchangerterminalrefside2, phasetapchangerregulating2
from threewindingstransformer
where phasetapchangerregulating2 is not null;

insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'RATIO_TAP_CHANGER_SIDE_TWO', ratiotapchangerregulationmode2, null, null, ratiotapchangerterminalrefconnectableid2, ratiotapchangerterminalrefside2, ratiotapchangerregulating2
from threewindingstransformer
where ratiotapchangerregulating2 is not null;
-- side 3
insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'PHASE_TAP_CHANGER_SIDE_THREE', phasetapchangerregulationmode3, null, null, phasetapchangerterminalrefconnectableid3, phasetapchangerterminalrefside3, phasetapchangerregulating3
from threewindingstransformer
where phasetapchangerregulating3 is not null;

insert into regulatingpoint (networkuuid, variantnum, regulatingequipmentid, regulatingequipmenttype, regulatingtapchangertype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside, regulating)
select networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'RATIO_TAP_CHANGER_SIDE_THREE', ratiotapchangerregulationmode3, null, null, ratiotapchangerterminalrefconnectableid3, ratiotapchangerterminalrefside3, ratiotapchangerregulating3
from threewindingstransformer
where ratiotapchangerregulating3 is not null;

UPDATE regulatingpoint r1
SET regulatedequipmenttype = regulatingequipmenttype
WHERE regulatedequipmenttype is null AND r1.regulatingequipmentid = r1.regulatingterminalconnectableid;

UPDATE regulatingpoint r1
SET regulatedequipmenttype = (select CASE
                                                    WHEN bat.id IS NOT NULL THEN 'BATTERY'
                                                    WHEN bbs.id IS NOT NULL THEN 'BUSBAR_SECTION'
                                                    WHEN ld.id IS NOT NULL THEN 'LOAD'
                                                    WHEN gen.id IS NOT NULL THEN 'GENERATOR'
                                                    WHEN sc.id IS NOT NULL THEN 'SHUNT_COMPENSATOR'
                                                    WHEN vsc.id IS NOT NULL THEN 'VSC_CONVERTER_STATION'
                                                    WHEN lcc.id IS NOT NULL THEN 'LCC_CONVERTER_STATION'
                                                    WHEN svc.id IS NOT NULL THEN 'STATIC_VAR_COMPENSATOR'
                                                    WHEN twt.id IS NOT NULL THEN 'TWO_WINDINGS_TRANSFORMER'
                                                    WHEN ttwt.id IS NOT NULL THEN 'THREE_WINDINGS_TRANSFORMER'
                                                    WHEN l.id IS NOT NULL THEN 'LINE'
                                                    WHEN hvdc.id IS NOT NULL THEN 'HVDC_LINE'
                                                    WHEN dl.id IS NOT NULL THEN 'DANGLING_LINE'
                                                    END
                                         FROM  regulatingpoint r  LEFT JOIN battery bat ON r.regulatingterminalconnectableid = bat.id and r.networkuuid = bat.networkuuid and r.variantnum = bat.variantnum
                                                                  LEFT JOIN busbarsection bbs ON r.regulatingterminalconnectableid = bbs.id and r.networkuuid = bbs.networkuuid and r.variantnum = bbs.variantnum
                                                                  LEFT JOIN load ld ON r.regulatingterminalconnectableid = ld.id and r.networkuuid = ld.networkuuid and r.variantnum = ld.variantnum
                                                                  LEFT JOIN generator gen ON r.regulatingterminalconnectableid = gen.id and r.networkuuid = gen.networkuuid and r.variantnum = gen.variantnum
                                                                  LEFT JOIN shuntcompensator sc ON r.regulatingterminalconnectableid = sc.id and r.networkuuid = sc.networkuuid and r.variantnum = sc.variantnum
                                                                  LEFT JOIN vscconverterstation vsc ON r.regulatingterminalconnectableid = vsc.id and r.networkuuid = vsc.networkuuid and r.variantnum = vsc.variantnum
                                                                  LEFT JOIN lccconverterstation lcc ON r.regulatingterminalconnectableid = lcc.id and r.networkuuid = lcc.networkuuid and r.variantnum = lcc.variantnum
                                                                  LEFT JOIN staticvarcompensator svc ON r.regulatingterminalconnectableid = svc.id and r.networkuuid = svc.networkuuid and r.variantnum = svc.variantnum
                                                                  LEFT JOIN twowindingstransformer twt ON r.regulatingterminalconnectableid = twt.id and r.networkuuid = twt.networkuuid and r.variantnum = twt.variantnum
                                                                  LEFT JOIN threewindingstransformer ttwt ON r.regulatingterminalconnectableid = ttwt.id and r.networkuuid = ttwt.networkuuid and r.variantnum = ttwt.variantnum
                                                                  LEFT JOIN line l ON r.regulatingterminalconnectableid = l.id and r.networkuuid = l.networkuuid and r.variantnum = l.variantnum
                                                                  LEFT JOIN hvdcline hvdc ON r.regulatingterminalconnectableid = hvdc.id and r.networkuuid = hvdc.networkuuid and r.variantnum = hvdc.variantnum
                                                                  LEFT JOIN danglingline dl ON r.regulatingterminalconnectableid = dl.id and r.networkuuid = dl.networkuuid and r.variantnum = dl.variantnum
                                         WHERE r1.networkuuid = r.networkuuid and  r1.variantnum = r.variantnum
                                           and r1.regulatingequipmentid = r.regulatingequipmentid and r1.regulatingequipmenttype = r.regulatingequipmenttype and r1.regulatingtapchangertype = r.regulatingtapchangertype)
WHERE regulatingterminalconnectableid is not null AND regulatedequipmenttype is null AND r1.regulatingequipmentid <> r1.regulatingterminalconnectableid;

UPDATE regulatingpoint SET regulationmode = REPLACE(regulationmode, '"', '') WHERE regulatingequipmenttype = 'THREE_WINDINGS_TRANSFORMER' or regulatingequipmenttype = 'TWO_WINDINGS_TRANSFORMER';