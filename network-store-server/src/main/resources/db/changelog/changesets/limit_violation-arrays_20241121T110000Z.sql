CREATE TYPE TemporaryLimitClass AS (
    operationallimitsgroupid VARCHAR(50),
    side INTEGER,
    limittype VARCHAR(50),
    name_ VARCHAR(255),
    value_ FLOAT8,
    acceptableduration INTEGER,
    fictitious BOOLEAN
    );

CREATE TABLE newtemporarylimits (
    equipmentid VARCHAR(255),
    equipmenttype VARCHAR(255),
    networkuuid VARCHAR(255),
    variantnum INTEGER,
    temporarylimits TemporaryLimitClass[]
);