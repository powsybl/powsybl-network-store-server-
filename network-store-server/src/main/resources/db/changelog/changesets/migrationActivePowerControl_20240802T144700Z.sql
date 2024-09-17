UPDATE extension
SET value_ = jsonb_set(cast(value_ as jsonb), '{minTargetP}', '"NaN"', true)
WHERE name='activePowerControl' and value_ !~ 'minTargetP';

UPDATE extension
SET value_ = jsonb_set(cast(value_ as jsonb), '{maxTargetP}', '"NaN"', true)
WHERE name='activePowerControl' and value_ !~ 'maxTargetP';
