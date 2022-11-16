## Stargate deployment using helm
Want to build Stargate on a k8s cluster using helm? Follow the instruction as below

## Pre-requisite

Cassandra installed with its storage port 7000 accessible as a headless service. \
install helm in the environment.

## Cassandra installation
For quick start, install cassandra in kubernetes cluster using helm. 
```shell script
 helm install my-release bitnami/cassandra
```

## Autoscaling
Autoscaling uses metrics server. Metrics server can be installed as:\
```shell script
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

## Ingress
To use Ingress, ingress controller needs to be installed and respective ingress class name has to be set for ingress.ingressClassName. By default it uses nginx ingress controller. This can be installed as: \
```shell script
 helm upgrade --install ingress-nginx ingress-nginx \
   --repo https://kubernetes.github.io/ingress-nginx \
   --namespace ingress-nginx --create-namespace 
```

When using ingress, api services path needs to be used as specified below


| API                 | Default path when using ingress                                                                   |
|---------------------|---------------------------------------------------------------------------------------------------|
| auth-api        | http://localhost/api/auth/v1/auth |
| rest-api      | http://localhost/api/rest/v2/schemas/keyspaces                                                              |
| docs-api | http://localhost/api/docs/v2/namespaces/test/collections/library                            |
| image.repository40  | http://localhost/api/graphql/graphql-schema                            |


## Helm installation instruction
Clone the stargate code from the github repository
```shell script
cd helm\
helm install stargate stargate
```

To install with override values
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

Note:
  - The helm values file (values.yaml) is updated with default values if cassandra is installed as - helm install my-release bitnami/cassandra \

| Helm value                                    | Description                                                                        | Default                                 |
|--------------------------------------------|------------------------------------------------------------------------------------|--------------------------------------------|
| replicaCount | Bridge/coordinator replica count. This is also the replication for CQL, Auth and GRPC end points. | 2 |
| image.registry              | Repo from where images are retrieved | docker.io |
| image.repository311        | Coordinator/Bridge image if persistence layer is Apache cassandra 3.11   | stargateio/coordinator-3_11 | 
| image.repository40              | Coordinator/Bridge image if persistence layer is Apache cassandra 4.0   | stargateio/coordinator-4_0 |
| image.repositoryDse68              | Coordinator/Bridge image if persistence layer is Dse 6.8   | stargateio/coordinator-4_0 |
| image.tag                  | Coordinator/Bridge image tag  | v2 |
| cassandra.clusterName                  | Deployed persistence cassandra cluster name  | cassandra |
| cassandra.dcName                  | Deployed persistence cassandra datacenten name  | datacenter1 |
| cassandra.rack                  | Deployed persistence cassandra rack  | rack1 |
| cassandra.seed                  | Headless service name that corresponds to Cassandra's storage port (7000)  | my-release-cassandra-headless.default.svc.cluster.local |
| cassandra.isDse                  | Set to true if DSE is used  | false |
| cassandra.clusterVersion                  | Cluster version is set as 3.11 for Cassandra 3x version, 4.0 for Cassandra 4x version and 6.8 for DSE Cassandra  | 4.0 |
| topologyKey | K8s node label to which coordinator/bridge service can be deployed | kubernetes.io/hostname |
| cpuReqMillicores                  | CPU request unit for bridge service  | 2000 |
| heapMB                  | Memory request unit for bridge service  | 2048 |
| restapi.enabled                  | Set to true if rest api need to be enabled  | true |
| restapi.replicaCount                  | Number of replica for rest api service  | 2 |
| restapi.cpu                  | CPU request unit for rest api service  | 2000 |
| restapi.memory                  | Memory request unit for rest api service  | 2048 |
| restapi.topologyKey | K8s node label to which rest api service can be deployed | kubernetes.io/hostname |
| docsapi.enabled                  | Set to true if document api need to be enabled  | true |
| docsapi.replicaCount                  | Number of replica for document api service  | 2 |
| docsapi.cpu                  | CPU request unit for document api service  | 2000 |
| docsapi.memory                  | Memory request unit for document api service  | 2048 |
| docsapi.topologyKey | K8s node label to which document service can be deployed | kubernetes.io/hostname |
| graphqlapi.enabled                  | Set to true if graphql api need to be enabled  | true |
| graphqlapi.replicaCount                  | Number of replica for graphql api service  | 2 |
| graphqlapi.cpu                  | CPU request unit for graphql api service  | 2000 |
| graphqlapi.memory                  | Memory request unit for graphql api service  | 2048 |
| graphqlapi.topologyKey | K8s node label to which graphql service can be deployed | kubernetes.io/hostname |
| autoscaling.enabled | Enable HPA this needs metrics server installed in the cluster | false |
| autoscaling.minReplicas | Minimum number of pod replicas to which it can be downsized | 2 |
| autoscaling.maxReplicas | Maximum number of pod replicas to which it can be scaled up | 100 |
| autoscaling.targetCPUUtilizationPercentage | Average target CPU to upscale or downscale | 80 |
| ingress.enabled | Set to true if ingress need to be enabled | false |
| ingress.ingressClassName | Default uses nginx controller, if different controller is used, appropriate class name needs to be updated | nginx |

