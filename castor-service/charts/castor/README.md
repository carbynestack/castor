# Castor

Carbyne Stack Castor service to store and manage tuples. Tuples are
cryptographic material that are consumed by other Carbyne Stack services in
order to provide mathematically proven security while transmitting, storing and
consuming data.

## TL;DR

```bash
# Testing configuration
$ helm install castor
```

## Introduction

This chart bootstraps a [Castor Service](https://github.com/carbynestack/castor)
deployment on a [Kubernetes](http://kubernetes.io) cluster using the
[Helm](https://helm.sh) package manager.

> **Tip**: This chart is used in the `helmfile.d` based deployment specification
> available in the top-level directory of this repository.

## Prerequisites

- Kubernetes 1.10+ (may also work on earlier versions but has not been tested)
- A Docker Hub account with read access permission for the organization
  `carbynestack` and a Kubernetes Secret with your registry credentials (see the
  section on [Registry Credentials](#registry-credentials)).
- A Redis RDS, Postgres DBMS, and MinIO cluster to serve as the persistence
  layer for Castor.

## Installing the Chart

To install the chart with the release name `my-release`:

```bash
helm install --name my-release castor
```

Make sure that your current working directory is
`<castor-service-base-dir>/charts`. The command deploys Castor on the Kubernetes
cluster in the default configuration. The [configuration](#configuration)
section lists the parameters that can be configured during installation.

> **Tip**: List all releases using `helm list`

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm delete my-release
```

The command removes all the Kubernetes components associated with the chart and
deletes the release.

## Configuration

The following table lists the (main) configurable parameters of the
`postgres-dbms` chart and their default values. For the full list of
configuration parameters see `values.yaml`. More information on the format
required for the parameters `users` and `databases` can be found in the
documentation for the
[Zalando postgres operator](https://github.com/zalando-incubator/postgres-operator).

| Parameter                                  | Description                                                                               | Default                                       |
|--------------------------------------------|-------------------------------------------------------------------------------------------| --------------------------------------------- |
| `castor.image.registry`                    | Castor Image registry                                                                     | `ghcr.io`                                     |
| `castor.image.repository`                  | Castor Image name                                                                         | `carbynestack/castor`                         |
| `castor.image.tag`                         | Castor Image tag                                                                          | `latest`                                      |
| `castor.image.pullPolicy`                  | Castor Image pull policy                                                                  | `IfNotPresent`                                |
| `castor.springActiveProfiles`              | Defines the Castor's Spring profiles to be loaded                                         | `k8s`                                         |
| `castor.isMaster`                          | Defines if Castor is running as a master service                                          | `true`                                        |
| `castor.slaveUris`                         | List of URIs for all Castor slave services                                                | \`\`                                          |
| `castor.initialFragmentSize`               | Defines the initial size of the fragments a chunk is split into                           | `1000`                                        |
| `castor.redis.host`                        | The host address to the redis key/value store                                             | `redis.default.svc.cluster.local`             |
| `castor.redis.port`                        | The port of the redis key/value store                                                     | `6379`                                        |
| `castor.minio.endpoint`                    | The minio object store endpoint                                                           | `http://minio.default.svc.cluster.local:9000` |
| `castor.db.host`                           | The postgres database host                                                                | `dbms-repl.default.svc.cluster.local`         |
| `castor.redis.port`                        | The postgres database port                                                                | `5432`                                        |
| `castor.db.userSecretName`                 | Name of an existing secret to be used for the database username                           | \`\`                                          |
| `castor.db.passwordSecretName`             | Name of an existing secret to be used for the database password                           | \`\`                                          |
| `castor.probes.liveness.initialDelay`      | Number of seconds after the container has started before the liveness probe is initiated  | `120`                                         |
| `castor.probes.liveness.period`            | How often (in seconds) to perform the liveness probe                                      | `10`                                          |
| `castor.probes.liveness.failureThreshold`  | How often to fail the liveness probe before finally be marked as unsuccessful             | `3`                                           |
| `castor.probes.readiness.initialDelay`     | Number of seconds after the container has started before the readiness probe is initiated | `0`                                           |
| `castor.probes.readiness.period`           | How often (in seconds) to perform the readiness probe                                     | `5`                                           |
| `castor.probes.readiness.failureThreshold` | How often to fail the readiness probe before finally be marked as unsuccessful            | `3`                                           |

Specify each parameter using the `--set key=value[,key=value]` argument to
`helm install`. For example,

```bash
helm install --name my-release \
  --set castor.image.tag=2018-10-16_15 \
    castor
```

The above command sets the Castor image version to `2018-10-16_15`.

Alternatively, a YAML file that specifies the values for the parameters can be
provided while installing the chart. For example,

```bash
helm install --name my-release -f values.yaml castor
```

> **Tip**: You can use the default [values.yaml](values.yaml)

## Registry Credentials

The Castor docker image is currently hosted on Azure Container Registry which
has a tight integration with the AKS. It allows worker nodes to pull docker
images from acr without explicitly specifying the credentials. As long as the
VCPs are deployed on AKS, no additional credentials are required.
