# Using Config Store Yaml

This extension provides support for loading stargate configuration from
a YAML file.

## YAML File Location

Before the `ConfigStoreYaml` is registered in the OSGi, it will try to
lookup the stargate config in the `/etc/stargate/stargate-config.yaml`location. 
Please note that the name of the actual YAML file is `stargate-config.yaml`. 
The Activator does not check if the file exists and always registers the `ConfigStoreYaml`. The validation is postponed to the runtime.
The config store lookup for config using the `stargate.config_store.yaml.location` system property that takes the
absolute path to the stargate config YAML file.

## YAML Config Format

The YAML file used to load the Stargate configuration should have the
following format:

```yaml
extension-1:
  a: 1
  b: "value"

extension-2:
  a: 2
  b: "value_2"
```

Every module should have its own dedicated section. When client is using
the `ConfigStore#getConfigForModule(String moduleName)` method to
retrieve the config for a given module, the `ConfigStoreYaml` returns
only a dedicated section. So for example, when using
`ConfigStore#getConfigForModule("extension-1)` it will return
`ConfigWithOverrides` backed by `Map<String, Object>` with two elements: `a:1` and `b:"value"`. 
It will allow isolation between settings from various modules.

If the caller will try to look up the config for an module that does not
have a dedicated section in the YAML file, it will throw the
`MissingModuleSettingsException`. This is a `RuntimeException` and it’s
the caller’s responsibility to catch it and handle gracefully or
propagate higher in the call stack.

If the caller tries to load a config file that does not exist, it will throw the
`UncheckedIOException` with the following message: `Problem when processing YAML file from: path_to_file`. 

## YAML File Cache

Loading the YAML file every time the `ConfigStore#getConfigForModule(String moduleName)` is called would add substantial performance overhead.
To reduce this, the file is cached for 30 seconds after write. It means that the change to the underlying `stargate-config.yaml`
will be captured by the `getConfigForModule()` method with 30 seconds delay.

The Config Store YAML exposes cache level metrics to allow tracking efficiency and performance. 
All metrics are exposed under `config.store.yaml` prefix. 
The `MetricRegistry` will have the following cache-level metrics:
*   `config.store.yaml.file-cache.hitCount`
*   `config.store.yaml.file-cache.hitRate`
*   `config.store.yaml.file-cache.missCount`
*   `config.store.yaml.file-cache.missRate`
*   `config.store.yaml.file-cache.evictionCount`
*   `config.store.yaml.file-cache.size`

## Using Config Store Yaml with Kubernetes(K8s)

This config store needs to have a YAML file in the local file system. It
can be provided in any way, but the main use case is to provide the YAML
using K8s. Let’s assume that the end-user has the `stargate-config.yaml`
ready, and he wants to deploy it using K8s.

### Create a config-map

The first step that the end-user need to do is to create a config-map
using the YAML file:
`kubectl create configmap from-yaml --from-file=./stargate-config.yaml`

### Mount into a Pod’s File System

Once the config map is registered in the K8s, it needs to be mounted
into a Pod’s file system:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: name-of-the-pod
spec:
  containers:
    - name: test-container
      image: k8s.gcr.io/busybox
      volumeMounts:
      - name: config-volume
        mountPath: /etc/stargate
  volumes:
    - name: config-volume
      configMap:
        # Provide the name of the ConfigMap containing the files you want
        # to add to the container
        name: from-yaml
  restartPolicy: Never
```

### Config Store YAML

Finally, when it is mounted into a file system, the `ConfigStoreActivator` can locate the file under:
`/etc/stargate/stargate-config.yaml` and start the `ConfigStoreYaml` service.
