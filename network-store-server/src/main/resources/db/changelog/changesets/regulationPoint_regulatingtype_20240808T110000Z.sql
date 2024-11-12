UPDATE regulationpoint r1
SET regulatingterminalconnectabletype = (select CASE
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
                                         FROM  regulationpoint r  LEFT JOIN battery bat ON r.regulatingterminalconnectableid = bat.id and r.networkuuid = bat.networkuuid and r.variantnum = bat.variantnum
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
                                           and r1.regulatedequipmentid = r.regulatedequipmentid and r1.equipmenttype = r.equipmenttype)
WHERE r1.regulatedequipmentid <> r1.regulatingterminalconnectableid;