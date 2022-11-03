## Stargate deployment using helm
Want to build Stargate on a k8s cluster using helm? Follow the instruction as below

## Pre-requisite

Cassandra storage port 7000 accessible as a k8s service.
install helm in the environment.

## Helm installation instruction
Clone the stargate repository  
cd helm  
helm install stargate stargate

Note:
  - The helm values file (values.yaml) is updated with default values if cassandra is installed as - helm install my-release bitnami/cassandra
  - Memory and CPU units provided in the values.yaml file is based on testing done on local environment with 6GB RAM and 4 CPU allocated to k8s environment.

## Autoscaling
Auto scaling uses metrics server. Metrics server can be installed as:\
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

## Ingress
Ingress requires ingress controller to be installed and appropriate class name updated to the helm values ingress.ingressClassName. By default it uses nginx controller. Nginx ingress controller can be installed as:\
 helm upgrade --install ingress-nginx ingress-nginx \
   --repo https://kubernetes.github.io/ingress-nginx \
   --namespace ingress-nginx --create-namespace \

When using ingress, path need to be appended with the service url as per example below

1)&emsp;&emsp; auth-api: &emsp;&emsp;&emsp;&emsp; http://localhost/api/auth/v1/auth \
2)&emsp;&emsp; rest-api: &emsp;&emsp;&emsp;&emsp; http://localhost/api/rest/v2/schemas/keyspaces \
3)&emsp;&emsp; docs-api: &emsp;&emsp;&emsp;&emsp; http://localhost/api/docs/v2/namespaces/test/collections/library \
4)&emsp;&emsp; graphql-api: &emsp;&emsp;&emsp;&emsp; http://localhost/api/graphql/graphql-schema \



## Helm values.yaml description

replicaCount: 1 -- Bridge relication count. This is also the replication for CQL, Auth and GRPC end points.

image:\
&emsp;&emsp;repository311: "stargateio/coordinator-3_11"\
&emsp;&emsp;repository40: "stargateio/coordinator-4_0"\
&emsp;&emsp;repositoryDse628: "stargateio/coordinator-dse-68"\
&emsp;&emsp;tag: "v2" -- image tag to be used for the deployment

cassandra:\
&emsp;&emsp;clusterName: "cassandra" -- Deployed cassandra cluster name\
&emsp;&emsp;dcName: "datacenter1"  -- Deployed cassandra datacenter name\
&emsp;&emsp;rack: "rack1" -- Deployed cassandra rack name\
&emsp;&emsp;seed: "my-release-cassandra-headless" -- Service name that corresponds to Cassandra's storage port\
&emsp;&emsp;isDse: null -- Set to true if DSE is used\
&emsp;&emsp;clusterVersion: "4.0" -- CLuster version is set as 3.11 for Cassandra 3x version, 4.0 for Cassandra 4x version and 6.8 for DSE Cassandra

cpuReqMillicores: 1000  -- CPU request unit for bridge service\
heapMB: 1024 -- Memory MB for bridge service

restapi:\
&emsp;&emsp;enabled: true -- Set to true if rest api need to be enabled\
&emsp;&emsp;replicaCount: 1 -- Number of replica for rest api service\
&emsp;&emsp;cpu: 500 -- CPU request unit for rest api service\
&emsp;&emsp;memory: 512 -- Memory request unit for rest api service

docsapi:\
&emsp;&emsp;enabled: true -- Set to true if document api need to be enabled\
&emsp;&emsp;replicaCount: 1 -- Number of replica for document api service\
&emsp;&emsp;cpu: 500 -- CPU request unit for document api service\
&emsp;&emsp;memory: 512 -- Memory request unit for document api service

graphqlapi:\
&emsp;&emsp;enabled: true -- Set to true if graphql api need to be enabled\
&emsp;&emsp;replicaCount: 1 -- Number of replica for graphql api service\
&emsp;&emsp;cpu: 500 -- CPU request unit for graphql api service\
&emsp;&emsp;memory: 512 -- CPU request unit for graphql api service

autoscaling:\
&emsp;&emsp;enabled: true -- Set to true if autoscaling need to be enabled\
&emsp;&emsp;minReplicas: 1 -- Cluster can be downsized to 1 pod for each deploymemnt
&emsp;&emsp;maxReplicas: 1 -- Cluster can be increased to 100 pod for each deploymemnt
&emsp;&emsp;targetCPUUtilizationPercentage: 80 -- Average percentage to increase the pod count for a deployment

ingress:\
&emsp;&emsp;enabled: true -- Set to true if ingress need to be enabled\
&emsp;&emsp;ingressClassName: nginx -- Default uses nginx controller, if different controller is used class name needs to be updated accordingly\

