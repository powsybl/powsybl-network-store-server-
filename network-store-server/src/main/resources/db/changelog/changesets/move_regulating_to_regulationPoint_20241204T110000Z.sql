-- generator
update regulatingpoint
set regulating = (select generator.voltageregulatoron from generator
where regulatingequipmenttype = 'GENERATOR'
  and regulatingequipmentid = generator.id
  and regulatingpoint.networkuuid = generator.networkuuid
  and regulatingpoint.variantnum = generator.variantnum);
-- vsc
update regulatingpoint
set regulating = (select vscconverterstation.voltageregulatoron
from vscconverterstation
where regulatingequipmenttype = 'VSC_CONVERTER_STATION'
  and regulatingequipmentid = vscconverterstation.id
  and regulatingpoint.networkuuid = vscconverterstation.networkuuid
  and regulatingpoint.variantnum = vscconverterstation.variantnum);
-- shunt
update regulatingpoint
set regulating = (select shuntcompensator.voltageregulatoron
from shuntcompensator
where regulatingequipmenttype = 'SHUNT_COMPENSATOR'
  and regulatingequipmentid = shuntcompensator.id
  and regulatingpoint.networkuuid = shuntcompensator.networkuuid
  and regulatingpoint.variantnum = shuntcompensator.variantnum);

-- svc
update regulatingpoint
set regulating = (CASE WHEN regulatingpoint.regulationmode = 'VOLTAGE'
                           THEN TRUE ELSE FALSE END)
where regulatingpoint.regulatingequipmenttype = 'STATIC_VAR_COMPENSATOR';