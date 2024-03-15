insert into permanentlimit (equipmentid, equipmenttype, networkuuid, variantnum, operationallimitsgroupid, side, limittype, value_)
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 1, 'CURRENT', permanentcurrentlimit1
from line
where permanentcurrentlimit1 is not null
union all
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 2, 'CURRENT', permanentcurrentlimit2
from line
where permanentcurrentlimit2 is not null
union all
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 1, 'ACTIVE_POWER', permanentactivepowerlimit1
from line
where permanentactivepowerlimit1 is not null
union all
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 2, 'ACTIVE_POWER', permanentactivepowerlimit2
from line
where permanentactivepowerlimit2 is not null
union all
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 1, 'APPARENT_POWER', permanentapparentpowerlimit1
from line
where permanentapparentpowerlimit1 is not null
union all
select id, 'LINE', networkuuid, variantnum, 'DEFAULT', 2, 'APPARENT_POWER', permanentapparentpowerlimit2
from line
where permanentapparentpowerlimit2 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'CURRENT', permanentcurrentlimit1
from twowindingstransformer
where permanentcurrentlimit1 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'CURRENT', permanentcurrentlimit2
from twowindingstransformer
where permanentcurrentlimit2 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'ACTIVE_POWER', permanentactivepowerlimit1
from twowindingstransformer
where permanentactivepowerlimit1 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'ACTIVE_POWER', permanentactivepowerlimit2
from twowindingstransformer
where permanentactivepowerlimit2 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'APPARENT_POWER', permanentapparentpowerlimit1
from twowindingstransformer
where permanentapparentpowerlimit1 is not null
union all
select id, 'TWO_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'APPARENT_POWER', permanentapparentpowerlimit2
from twowindingstransformer
where permanentapparentpowerlimit2 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'CURRENT', permanentcurrentlimit1
from threewindingstransformer
where permanentcurrentlimit1 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'CURRENT', permanentcurrentlimit2
from threewindingstransformer
where permanentcurrentlimit2 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 3, 'CURRENT', permanentcurrentlimit3
from threewindingstransformer
where permanentcurrentlimit3 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'ACTIVE_POWER', permanentactivepowerlimit1
from threewindingstransformer
where permanentactivepowerlimit1 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'ACTIVE_POWER', permanentactivepowerlimit2
from threewindingstransformer
where permanentactivepowerlimit2 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 3, 'ACTIVE_POWER', permanentactivepowerlimit3
from threewindingstransformer
where permanentactivepowerlimit3 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 1, 'APPARENT_POWER', permanentapparentpowerlimit1
from threewindingstransformer
where permanentapparentpowerlimit1 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 2, 'APPARENT_POWER', permanentapparentpowerlimit2
from threewindingstransformer
where permanentapparentpowerlimit2 is not null
union all
select id, 'THREE_WINDINGS_TRANSFORMER', networkuuid, variantnum, 'DEFAULT', 3, 'APPARENT_POWER', permanentapparentpowerlimit3
from threewindingstransformer
where permanentapparentpowerlimit3 is not null
union all
select id, 'DANGLING_LINE', networkuuid, variantnum, 'DEFAULT', 1, 'CURRENT', permanentcurrentlimit
from danglingline
where permanentcurrentlimit is not null
union all
select id, 'DANGLING_LINE', networkuuid, variantnum, 'DEFAULT', 1, 'ACTIVE_POWER', permanentactivepowerlimit
from danglingline
where permanentactivepowerlimit is not null
union all
select id, 'DANGLING_LINE', networkuuid, variantnum, 'DEFAULT', 1, 'APPARENT_POWER', permanentapparentpowerlimit
from danglingline
where permanentapparentpowerlimit is not null;