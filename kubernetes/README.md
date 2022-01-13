# Simple Kubernetes Deployment of Stargate

[K8ssandra](https://k8ssandra.io) site

Make sure you have a Kubernetes cluster available with minimum resources.

Must have a storage class is available with the required VOLUMEBINDINGMODE of WaitForFirstConsumer:

```
kubectl get storageclasses
```

Next make sure you have helm installed and add the K8ssandra repo

```
helm repo add k8ssandra https://helm.k8ssandra.io/stable
```

Now install the chart (or use the provided script)

```
helm install -f k8ssandra-values.yaml k8ssandra-sg k8ssandra/k8ssandra
```


