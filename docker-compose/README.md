# Stargate Docker Compose Scripts
This directory provides Docker compose scripts with sample configurations for the various supported Stargate backends:

- [Cassandra 3.11](cassandra-3.11)
- [Cassandra 4.0](cassandra-4.0)
- [DataStax Enterprise 6.8](dse-6.8)

Stargate Docker images are publicly available on [Docker Hub](https://hub.docker.com/r/stargateio/). To learn how the images are built or to build your own local versions, see the [coordinator node developer guide](../DEV_GUIDE.md) or API Service developer guides (under the [apis](apis) directory).

## Docker Troubleshooting

If you have problems running Stargate Docker images there are couple of things you can check that might be causing these problems.

### Docker Engine does not have enough memory to run full Stargate

Many Docker engines have default settings for low memory usage. For example:

* Docker Desktop defaults to 2 GB: depending on your set up, you may need to increase this up to 6 GBs (although some developers report 4 GB being sufficient)

### MacOS has too low limit for File Descriptors

MacOS has low `ulimit` for maximum number of files open. If you see a message like this:

```
Jars will not be watched due to unexpected error: User limit of inotify instances reached or too many open files
```

on Docker logs for Coordinator, you are probably hitting it.
The solution is to increase `maxfiles` limit; you can do that by:

```
sudo launchctl limit maxfiles 999999 999999
```

and optionally adding following entry in `/etc/launchd.conf` file to reset values on restart
(file might not exist; if so, just create it with this line)

```
limit maxfiles 999999 999999
```

For more information see f.ex:

https://wilsonmar.github.io/maximum-limits/
