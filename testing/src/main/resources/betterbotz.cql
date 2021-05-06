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
    "value_int" int,
    "value_double" double,
    "value_bigint" bigint,
    "value_varint" varint,
    "value_float" float,
    "value_smallint" smallint,
    "value_tinyint" tinyint,

    PRIMARY KEY (("prodName"), "customerName")
);

CREATE TABLE IF NOT EXISTS betterbotz."CollectionsSimple" (
    id uuid PRIMARY KEY,
    "listValue1" frozen<list<int>>,
    "listValue2" frozen<list<timeuuid>>,
    "setValue1" frozen<set<text>>,
    "mapValue1" frozen<map<int, text>>,
    "mapValue2" frozen<map<uuid, bigint>>,
);

CREATE TABLE IF NOT EXISTS betterbotz."CollectionsNested" (
    id uuid PRIMARY KEY,
    "listValue1" frozen<list<frozen<map<int, text>>>>,
    "setValue1" frozen<list<frozen<set<uuid>>>>,
    "mapValue1" frozen<map<uuid, frozen<map<bigint, text>>>>
);

CREATE TYPE IF NOT EXISTS betterbotz."B"(i int);

CREATE TYPE IF NOT EXISTS betterbotz."A"(b frozen<"B">);

CREATE TABLE IF NOT EXISTS betterbotz."Udts"(a frozen<"A"> PRIMARY KEY, bs list<frozen<"B">>);

CREATE TABLE IF NOT EXISTS betterbotz."Tuples"(
    id uuid PRIMARY KEY,
    tuple1 tuple<bigint>,
    tuple2 tuple<float, float>,
    tuple3 tuple<timeuuid, int, boolean>
);

CREATE TABLE IF NOT EXISTS betterbotz."TuplesPk"(id tuple<int, int> PRIMARY KEY);

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
