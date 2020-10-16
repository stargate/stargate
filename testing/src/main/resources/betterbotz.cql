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

CREATE TYPE betterbotz.b(i int);

CREATE TYPE betterbotz.a(b frozen<b>);

CREATE TABLE betterbotz.udts(a frozen<a> PRIMARY KEY, bs list<frozen<b>>);
