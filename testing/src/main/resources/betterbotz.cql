-- Used to create the cql schema to generate the schema.json file
-- See GraphqlTest.java

CREATE KEYSPACE IF NOT EXISTS betterbotz
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS betterbotz."Products" (
    id uuid,
    name text,
    price decimal,
    created timestamp,
    "prodName" text,
    "customerName" text,
    description text,
    PRIMARY KEY ((id), name, price, created)
);

CREATE TABLE IF NOT EXISTS betterbotz."Orders" (
    "prodName" text,
    "customerName" text,
    id uuid,
    "prodId" uuid,
    address text,
    description text,
    price decimal,
    "sellPrice" decimal,
    PRIMARY KEY (("prodName"), "customerName")
);


CREATE TABLE IF NOT EXISTS betterbotz."Scalars" (
    id uuid PRIMARY KEY,
    asciivalue ascii,
    bigintvalue bigint,
    blobvalue blob,
    booleanvalue boolean,
    datevalue date,
    decimalvalue decimal,
    doublevalue double,
    durationvalue duration,
    floatvalue float,
    inetvalue inet,
    intvalue int,
    smallintvalue smallint,
    textvalue text,
    timevalue time,
    timestampvalue timestamp,
    timeuuidvalue timeuuid,
    tinyintvalue tinyint,
    uuidvalue uuid,
    varcharvalue varchar,
    varintvalue varint
);
