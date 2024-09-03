insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, equipmenttype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, 'STATIC_VAR_COMPENSATOR', regulationmode, id, null, regulatingterminal, null
from staticvarcompensator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, equipmenttype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, 'GENERATOR', null, id, null, regulatingterminal, null
from generator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, equipmenttype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, 'SHUNT_COMPENSATOR', null, id, null, regulatingterminal, null
from shuntcompensator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, equipmenttype, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, 'HVDC_CONVERTER_STATION', null, id, null, regulatingterminal, null
from vscconverterstation;