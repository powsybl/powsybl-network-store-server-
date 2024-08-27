insert into regulationpoint (networkuuid, variantnum, regulatedequipmentid, regulationmode, localterminalconnectableid, localterminalside, regulatingterminalconnectableid, regulatingterminalside)
select networkuuid, variantnum, id, regulationmode, id, null, regulatingterminal, null
from staticvarcompensator;