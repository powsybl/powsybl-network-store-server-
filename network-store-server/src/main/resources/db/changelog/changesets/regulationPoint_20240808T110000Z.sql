insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, regulationmode, id, null, regulatingterminal, null
from staticvarcompensator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, null, id, null, regulatingterminal, null
from generator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, null, id, null, regulatingterminal, null
from shuntcompensator;
insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, null, id, null, regulatingterminal, null
from vscconverterstation;