-- Used to create the cql schema to generate the schema.json file
-- See GraphqlTest.java

CREATE KEYSPACE IF NOT EXISTS betterbotz
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS betterbotz.products (
    id uuid,
    name text,
    price decimal,
    created timestamp,
    prod_name text,
    customer_name text,
    description text,
    PRIMARY KEY ((id), name, price, created)
);

CREATE TABLE IF NOT EXISTS betterbotz.orders (
    prod_name text,
    customer_name text,
    id uuid,
    prod_id uuid,
    address text,
    description text,
    price decimal,
    sell_price decimal,
    PRIMARY KEY ((prod_name), customer_name)
);

CREATE TABLE IF NOT EXISTS betterbotz.collections_simple (
    id uuid PRIMARY KEY,
    list_value1 frozen<list<int>>,
    list_value2 frozen<list<timeuuid>>,
    set_value1 frozen<set<text>>,
    map_value1 frozen<map<int, text>>,
    map_value2 frozen<map<uuid, bigint>>,
);

CREATE TABLE IF NOT EXISTS betterbotz.collections_nested (
    id uuid PRIMARY KEY,
    list_value1 frozen<list<frozen<map<int, text>>>>,
    set_value1 frozen<list<frozen<set<uuid>>>>,
    map_value1 frozen<map<uuid, frozen<map<bigint, text>>>>
);

CREATE TYPE IF NOT EXISTS betterbotz.b(i int);

CREATE TYPE IF NOT EXISTS betterbotz.a(b frozen<b>);

CREATE TABLE IF NOT EXISTS betterbotz.udts(a frozen<a> PRIMARY KEY, bs list<frozen<b>>);

CREATE TABLE IF NOT EXISTS betterbotz.scalars (
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
