# Stargate V2 in-Coordinator RESTv1 API

Stargate V1 used to provide in-Coordination API implementations via this module: Documents API (v2), REST API (v1, v2).
In Stargate V2 two of these -- Documents API v2, REST API v2 -- have been refactored into separate [API modules](https://github.com/stargate/stargate/tree/main/apis).
API endpoints for these APIs do not change but due split to separate services they need to be accessed from hosts different from Coordinator.

However, REST API v1 (`/v1/keyspaces`) implementation will still be included in Coordinator for backwards-compatibility purposes for Stargate V2.
REST API v1 is considered **DEPRECATED** and **will be removed from Stargate V3**
