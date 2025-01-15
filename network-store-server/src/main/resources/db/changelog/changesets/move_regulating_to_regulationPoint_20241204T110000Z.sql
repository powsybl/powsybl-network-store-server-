-- generator
update regulatingpoint
set regulating = (select generator.voltageregulatoron from generator
            where regulatingpoint.regulatingequipmenttype = 'GENERATOR'
              and regulatingpoint.regulatingequipmentid = generator.id
              and regulatingpoint.networkuuid = generator.networkuuid
              and regulatingpoint.variantnum = generator.variantnum)
where regulatingequipmenttype = 'GENERATOR';
-- vsc

update regulatingpoint
set regulating = (select vscconverterstation.voltageregulatoron from vscconverterstation
            where regulatingpoint.regulatingequipmenttype = 'VSC_CONVERTER_STATION'
              and regulatingpoint.regulatingequipmentid = vscconverterstation.id
              and regulatingpoint.networkuuid = vscconverterstation.networkuuid
              and regulatingpoint.variantnum = vscconverterstation.variantnum)
where regulatingequipmenttype = 'VSC_CONVERTER_STATION';
-- shunt
update regulatingpoint
set regulating = (select shuntcompensator.voltageregulatoron from shuntcompensator
            where regulatingpoint.regulatingequipmenttype = 'SHUNT_COMPENSATOR'
              and regulatingpoint.regulatingequipmentid = shuntcompensator.id
              and regulatingpoint.networkuuid = shuntcompensator.networkuuid
              and regulatingpoint.variantnum = shuntcompensator.variantnum)
where regulatingequipmenttype = 'SHUNT_COMPENSATOR';
-- svc
update regulatingpoint
set regulating = (CASE WHEN regulationmode = 'VOLTAGE'
                           THEN TRUE ELSE FALSE END)
where regulatingequipmenttype = 'STATIC_VAR_COMPENSATOR';