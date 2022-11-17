# Stargate deployment using Helm
Want to deploy Stargate on a Kubernetes cluster using Helm? Follow the instructions below. 

## Pre-requisites

You'll need a Cassandra cluster running with the storage port 7000 accessible as a headless service, and make sure [Helm](https://helm.sh) is available in the environment.

### Cassandra installation
For a quick start, you can install Cassandra in your Kubernetes cluster using Helm.

```shell script
 helm install my-release bitnami/cassandra
```

### Autoscaling
Autoscaling uses metrics server. Metrics server can be installed by executing the command:

```shell script
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### Ingress
To expose access to Stargate APIs from clients outside your Kubernetes cluster, make sure an ingress controller is installed. To install the Nginx ingress controller, execute the command:

```shell script
 helm upgrade --install ingress-nginx ingress-nginx \
   --repo https://kubernetes.github.io/ingress-nginx \
   --namespace ingress-nginx --create-namespace 
```

You'll use the name of ingress class name as the `ingress.ingressClassName` in the Stargate Helm chart values.

When using ingress, the API service paths need to be set as specified in the table

| API         | Default path when using ingress                                                                   |
|-------------|---------------------------------------------------------------------------------------------------|
| auth-api    | http://localhost/api/auth/v1/auth |
| rest-api    | http://localhost/api/rest/v2/schemas/keyspaces                                                              |
| docs-api    | http://localhost/api/docs/v2/namespaces/test/collections/library                            |
| graphql-api | http://localhost/api/graphql/graphql-schema                            |


## Helm installation 
Clone this repo to your development machine. Then execute the following commands to install the Stargate Helm chart with default values:

```shell script
cd helm
helm install stargate stargate
```

Note:
- The default values in the Helm values file (`values.yaml`) are set to assume that Cassandra is installed via `helm install my-release bitnami/cassandra`

To install with overriden values, you can use the `--set` option as shown below:

```shell script
helm install stargatev2 \
--namespace <ENTER_NAMESPACE_HERE> \
--set replicaCount=2 \
--set cassandra.clusterName=<ENTER_VALUE_HERE> \
--set cassandra.dcName=<ENTER_VALUE_HERE> \
--set cassandra.rack=<ENTER_VALUE_HERE> \
--set cassandra.seed=<ENTER_VALUE_HERE> \
--set cassandra.clusterVersion=<ENTER_VALUE_HERE> \
--set restapi.enabled=true \
--set restapi.replicaCount=2 \
--set docsapi.enabled=true \
--set docsapi.replicaCount=2 \
--set graphqlapi.enabled=true \
--set graphqlapi.replicaCount=2
```

The table below lists the configurable values supported by this chart:

| Helm value                                    | Description                                                                                                     | Default                                                  |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `replicaCount` | Coordinator replica count. This is also the replication for CQL, Auth and GRPC end points.                      | `2`                                                      |
| `image.registry`              | Repo from where images are retrieved                                                                            | `docker.io`                                              |
| `image.repository311`        | Coordinator image if persistence layer is Apache Cassandra 3.11                                          | `stargateio/coordinator-3_11`                            | 
| `image.repository40`              | Coordinator image if persistence layer is Apache Cassandra 4.0                                           | `stargateio/coordinator-4_0`                             |
| `image.repositoryDse68`              | Coordinator image if persistence layer is DataStax Enterprise 6.8                                        | `stargateio/coordinator-dse-68`                          |
| `image.tag`                  | Coordinator image tag                                                                                    | `v2`                                                     |
| `cassandra.clusterName`                  | Deployed Cassandra cluster name                                                                                 | `cassandra`                                              |
| `cassandra.dcName`                  | Deployed Cassandra datacenter name                                                                              | `datacenter1`                                            |
| `cassandra.rack`                  | Deployed Cassandra rack                                                                                         | `rack1`                                                  |
| `cassandra.seed`                  | Headless service name that corresponds to Cassandra's storage port (7000)                                       | `my-release-cassandra-headless.default.svc.cluster.local` |
| `cassandra.isDse`                  | Set to true if DSE is used                                                                                      | `false`                                                  |
| `cassandra.clusterVersion`                  | Cluster version is set as 3.11 for Cassandra 3x version, 4.0 for Cassandra 4x version and 6.8 for DSE Cassandra | `4.0`                                                    |
| `topologyKey` | K8s node label to which Stargate Coordinator can be deployed                                                    | `kubernetes.io/hostname`                                 |
| `cpuReqMillicores`                  | CPU request unit for Coordinator                                                                                | `2000`                                                   |
| `heapMB`                  | Memory request unit for Coordinator in MB                                                                       | `2048`                                                   |
| `restapi.enabled`                  | Set to true to enable REST API                                                                                  | `true`                                                   |
| `restapi.replicaCount`                  | Number of replicas for REST API Service                                                                         | `2`                                                      |
| `restapi.cpu`                  | CPU request unit for REST API Service in MB                                                                     | `2000`                                                   |
| `restapi.memory`                  | Memory request unit for REST API Service                                                                        | `2048`                                                   |
| `restapi.topologyKey` | K8s node label to which rest api service can be deployed                                                        | `kubernetes.io/hostname`                                 |
| `docsapi.enabled`                  | Set to true to enable Document API                                                                              | `true`                                                   |
| `docsapi.replicaCount`                  | Number of replicas for Document API Service                                                                     | `2`                                                        |
| `docsapi.cpu`                  | CPU request unit for document api service                                                                       | `2000`                                                     |
| `docsapi.memory`                  | Memory request unit for document api service                                                                    | `2048`                                                     |
| `docsapi.topologyKey` | K8s node label to which document service can be deployed                                                        | `kubernetes.io/hostname`                                   |
| `graphqlapi.enabled`                  | Set to true to enable GraphQL API                                                                               | `true`                                                     |
| `graphqlapi.replicaCount`                  | Number of replicas for GraphQL API Service                                                                      | `2`                                                        |
| `graphqlapi.cpu`                  | CPU request unit for GraphQL API Service                                                                        | `2000`                                                     |
| `graphqlapi.memory`                  | Memory request unit for GraphQL API Service in MB                                                               | 2048                                                     |
| `graphqlapi.topologyKey` | K8s node label to which graphql service can be deployed                                                         | `kubernetes.io/hostname`                                   |
| `autoscaling.enabled` | `Enable Horizontal Pod Autoscaler (requires metrics server to be installed in the cluster)                      | `false`                                                    |
| `autoscaling.minReplicas` | Minimum number of pod replicas                                                                                  | 2                                                        |
| `autoscaling.maxReplicas` | Maximum number of pod replicas                                                                                  | `100`                                                    |
| `autoscaling.targetCPUUtilizationPercentage` | Average target CPU utilization to trigger upscaling or downscaling                                              | `80`                                                     |
| `ingress.enabled` | Set to true to enable ingress                                                                                   | `false`                                                  |
| `ingress.ingressClassName` | Ingress controller class name                                                                                   | `nginx`                                                  |

