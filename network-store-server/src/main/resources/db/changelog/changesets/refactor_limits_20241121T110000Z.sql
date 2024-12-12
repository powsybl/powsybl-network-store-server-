-- will be used in next deployment
INSERT INTO newtemporarylimits (equipmentid, equipmenttype, networkuuid, variantnum, temporarylimits)
SELECT equipmentid, equipmenttype, networkuuid, variantnum,
       json_agg(json_build_object(
               'operationalLimitsGroupId', operationalLimitsGroupId,
               'side', side,
               'limitType', limitType,
               'name', name,
               'value', value_,
               'acceptableDuration', acceptableduration,
               'fictitious', fictitious
                )) as temporarylimits FROM temporarylimit
GROUP  BY equipmentid, equipmenttype, networkuuid, variantnum;
INSERT INTO newpermanentlimits (equipmentid, equipmenttype, networkuuid, variantnum, permanentlimits)
SELECT equipmentid, equipmenttype, networkuuid, variantnum,
       json_agg(json_build_object(
               'operationalLimitsGroupId', operationalLimitsGroupId,
               'value', value_,
               'side', side,
               'limitType', limitType
                )) as permanentlimits FROM permanentlimit
GROUP  BY equipmentid, equipmenttype, networkuuid, variantnum;