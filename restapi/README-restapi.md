# Stargate V1 REST API

**NOTE ON DEPRECATION**

Stargate V1 API implementations, including REST API are deprecated as of Stargate 2.x and will be removed from Stargate 3.x.

Note that this does not mean removal of REST API itself but re-architecting it so that APIs are not deployed as a part of the Coordinator but as separate stand-alone Services.

In addition, however, part of REST API -- called "RESTv1" -- is **ALSO** deprecated and scheduled for removal.

## General

In Stargate V1, the REST API module is an OSGi bundle that exposes a set of CRUD endpoints over HTTP,
for both Schema operations (DDL) and data manipulation (DML).

Here is a minimal overview of the service (for more details, refer to the [Stargate online docs]).

### RESTv1

Endpoints under `/v1/keyspaces` are the oldest part of REST API. Its use is strongly discouraged
and RESTv2 should be used instead as it is a complete replacement, a superset of functionality.

This part of API is DEPRECATED as of Stargate V2 and will be fully removed from Stargate V3.

### RESTv2

Endpoints under `/v2/schemas` (DDL) and `/v2/keyspaces` (DML) are the replacement for RESTv1 part of API and were included in Stargate V1. This part of API is fully supported and should be used instead of RESTv1.
