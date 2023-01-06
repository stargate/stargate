# Stargate V2 in-Coordinator RESTv1 API

## Differences from Stargate V1

Stargate V1 used to provide three in-Coordination API implementations via this module: Documents API (v2), REST API v1 and REST API v2).
In Stargate V2 two of these -- Documents API v2, REST API v2 -- have been refactored into separate [API modules](https://github.com/stargate/stargate/tree/main/apis).
API endpoints for these APIs do not change but due split to separate services, separate deployment, host to use will change (they are no longer accessed via Coordinator host(s)).
These endpoints are not accessible when using default StargateV2 deployment setup.

REST API v1 (`/v1/keyspaces`) implementation will, however, still be included in Coordinator for backwards-compatibility purposes in Stargate V2.
REST API v1 is considered **DEPRECATED** and **will be removed from Stargate V3**.

## Current status (v2.0.4)

Current version (v2.0.4) can still include RESTv2 and DocsV2 API implementations if (and only if) System Property

   stargate.rest.enableV1

is set to `true`. If so, following endpoints WILL be registered to be accessible via Coordinator node:

* REST API v2:
    * `/v2/keyspaces/`: DML
    * `/v2/schemas/`: DDL
* Documents API v2:
    * `/v2/namespaces/`: DML and DDL

Note, however, that this inclusion mechanism is only provided temporarily for the first 2.0.x releases.
It will be removed during 2.x release cycle once the new architecture's functioning has been fully verified.
