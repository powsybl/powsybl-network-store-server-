INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'BATTERY', 'activePowerControl',
       jsonb_set(
               COALESCE(activepowercontrol::jsonb, '{}'::jsonb),
               '{@type}',
               '"activePowerControl"'::jsonb
       )
FROM battery
WHERE activepowercontrol IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'GENERATOR', 'activePowerControl',
       jsonb_set(
               COALESCE(activepowercontrol::jsonb, '{}'::jsonb),
               '{@type}',
               '"activePowerControl"'::jsonb
       )
FROM generator
WHERE activepowercontrol IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'GENERATOR', 'startup',
       jsonb_set(
               COALESCE(generatorstartup::jsonb, '{}'::jsonb),
               '{@type}',
               '"startup"'::jsonb
       )
FROM generator
WHERE generatorstartup IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'LINE', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM line
WHERE operatingStatus IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'HVDC_LINE', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM hvdcline
WHERE operatingStatus IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'TIE_LINE', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM tieline
WHERE operatingStatus IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'DANGLING_LINE', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM danglingline
WHERE operatingStatus IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'TWO_WINDINGS_TRANSFORMER', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM twowindingstransformer
WHERE operatingStatus IS NOT NULL;

INSERT INTO extension (networkuuid, variantnum, equipmentid, equipmentType, name, value_)
SELECT networkuuid, variantnum, id, 'THREE_WINDINGS_TRANSFORMER', 'operatingStatus',
       jsonb_build_object(
               '@type', 'operatingStatus',
               'operatingStatus', operatingStatus::text
       )
FROM threewindingstransformer
WHERE operatingStatus IS NOT NULL;