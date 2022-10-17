## Stargate deployment using helm
Want to build Stargate on a k8s cluster using helm? Follow the instruction as below

## Pre-requisite

Cassandra storage port 7000 accessible as a k8s service.
install helm in the environment.

## Helm installation instruction
Clone the git repository.

cd helm

helm install stargate stargate

Note:
  - The helm values file (values.yaml) is updated with default values if cassandra is installed as - helm install my-release bitnami/cassandra
  - Memory and CPU units provided in the values.yaml file is based on testing done on a laptop with 6GB RAM and 4 CPU allocated to k8s environment.

## Helm values.yaml description

replicaCount: 1 -- Bridge relication count. This is also the replication for CQL, Auth and GRPC end points.

image:\
  repository311: "stargateio/coordinator-3_11"\
  repository40: "stargateio/coordinator-4_0"\
  repositoryDse628: "stargateio/coordinator-dse-68"\
  tag: "v2" -- image tag to be used for the deployment\

cassandra:\
  clusterName: "cassandra" -- Deployed cassandra cluster name\
  dcName: "datacenter1"  -- Deployed cassandra datacenter name\
  rack: "rack1" -- Deployed cassandra rack name\
  seed: "my-release-cassandra-headless" -- Service name that corresponds to Cassandra's storage port\
  isDse: null -- Set to true if DSE is used\
  clusterVersion: "4.0" -- CLuster version is set as 3.11 for Cassandra 3x version, 4.0 for Cassandra 4x version and 6.8 for DSE Cassandra\

cpuReqMillicores: 1000  -- CPU request unit for bridge service\
heapMB: 1024 -- Memory MB for bridge service\

restapi:\
  enabled: true -- Set to true if rest api need to be enabled\
  replicaCount: 1 -- Number of replica for rest api service\
  cpu: 500 -- CPU request unit for rest api service\
  memory: 512 -- Memory request unit for rest api service\

docsapi:\
  enabled: true -- Set to true if document api need to be enabled\
  replicaCount: 1 -- Number of replica for document api service\
  cpu: 500 -- CPU request unit for document api service\
  memory: 512 -- Memory request unit for document api service\

graphqlapi:\
  enabled: true -- Set to true if graphql api need to be enabled\
  replicaCount: 1 -- Number of replica for graphql api service\
  cpu: 500 -- CPU request unit for graphql api service\
  memory: 512 -- CPU request unit for graphql api service\
