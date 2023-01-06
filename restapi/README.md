# Stargate V2 in-Coordinator RESTv1 API

Stargate V1 used to provide three in-Coordination API implementations via this module: Documents API (v2), REST API v1 and REST API v2).
In Stargate V2 two of these -- Documents API v2, REST API v2 -- have been refactored into separate [API modules](https://github.com/stargate/stargate/tree/main/apis).
API endpoints for these APIs do not change but due split to separate services, separate deployment, host to use will change (they are no longer accessed via Coordinator host(s)).

However, REST API v1 (`/v1/keyspaces`) implementation will still be included in Coordinator for backwards-compatibility purposes for Stargate V2.
REST API v1 is considered **DEPRECATED** and **will be removed from Stargate V3**
