UPDATE network
SET cgmesmetadatamodels =
       jsonb_build_array(
           jsonb_build_object('subset', 'STATE_VARIABLES',
                              'id', '',
                              'description', cgmessvmetadata::json->'description',
                              'version', cgmessvmetadata::json->'svVersion',
                              'modelingAuthoritySet', cgmessvmetadata::json->'modelingAuthoritySet',
                              'profiles', jsonb_build_array(),
                              'dependentOn', cgmessvmetadata::json->'dependencies',
                              'supersedes', jsonb_build_array()
                              ),
           jsonb_build_object('subset', 'STEADY_STATE_HYPOTHESIS',
                              'id', cgmessshmetadata::json->'id',
                              'description', cgmessshmetadata::json->'description',
                              'version', cgmessshmetadata::json->'sshVersion',
                              'modelingAuthoritySet', cgmessshmetadata::json->'modelingAuthoritySet',
                              'profiles', jsonb_build_array(),
                              'dependentOn', cgmessshmetadata::json->'dependencies',
                              'supersedes', jsonb_build_array()
                              )
       )
WHERE cgmessvmetadata IS NOT NULL AND cgmessshmetadata IS NOT NULL;
