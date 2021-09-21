# Castor Service - A Storage Service for Cryptographic Material

The Castor service (hereinafter referred to as Castor for short) is a storage
service to manage cryptographic material, so called _tuples_, in the
[Carbyne Stack](https://github.com/carbynestack) platform. Tuples are a special
type of cryptographic material used for fast and secure execution of operations,
like secret sharing or distributed multiplications, in SPDZ-like Secure
Multiparty Computation protocols.

## Coordination

_Castor_ operates in a Master/Slave setting. In order for methods that consume
tuples for their execution to produce valid results, it must be ensured that the
consuming method receives matching shares of the same tuple from all parties in
the Virtual Cloud. This is ensured internally by so-called reservations, which
are managed by the master service. As soon as the master receives a request for
tuples, it creates such a reservation and informs all other parties in the
Virtual Cloud (slaves) which tuples are to be used for a specific request. The
master returns its shares of the assigned tuples after successfully forwarding
the reservation. If a request for tuples is received by a slave service, it
first checks whether a reservation exists for the given request. If this is the
case, it delivers the corresponding shares. If not, the service waits a certain
time to see if a corresponding reservation arrives or returns an error
otherwise.

## Endpoints

Castor provides the following two categories of endpoints:

- **Intra VCP**: Used by services operating on the same Virtual Cloud Provider
  to access the telemetry information, or individual tuple shares e.g. for being
  used in computations (`/intra-vcp`)
- **Inter VCP**: Used for communication with other Virtual Cloud Providers in
  the same Virtual Cloud as e.g. for sharing reservations (`/inter-vcp`)

> :warning: When setting up the system, it is necessary to limit access to
> non-public endpoints (`/intra-vcp` and `/inter-vcp`) to local services or the
> services provided by remote Virtual Cloud Providers, respectively.

Please note that Castor does not provide an interface to the clients of a
Virtual Cloud. Input tuples required for up- and downloading secrets are
channeled through Amphora.

## Getting Started

Castor is part of [Carbyne Stack](https://github.com/carbynestack) and only one
of multiple services each provider needs to run in order to participate in a
Virtual Cloud. The recommended way to start a Virtual Cloud locally for
development is using the [Carbyne Stack SDK](https://github.com/carbynestack).
Nevertheless, Castor can also be run in isolation, e.g. using helm (see
[charts/castor/README.md](charts/castor/README.md) for further details), or
using docker directly (see below).

### Docker Image

A docker image is available in the GitHub Container Repository. The latest image
can be pulled using:

```bash
docker pull ghcr.io/carbynestack/castor-service:latest
```

### Build from Source

Castor uses [Maven](https://maven.apache.org) for build automation and
dependency management.

To build a custom Castor docker image, run:

```bash
mvn clean package docker:build
```

### Deploy locally

In order to deploy Castor locally, the following additional services are
required:

- **Minio**: To persist the tuple shares (tuple chunks).
- **PostgreSQL**: To store the metadata of a tuple chunk.
- **Redis**: To cache tuple reservations as well as telemetry data.

#### Configuration

Castor requires a set of configuration parameters in order to run successful.
Please see
[charts/castor/README.md#configuration](charts/castor/README.md#configuration)
for a complete set of configuration parameters.

The following example shows how to start a Castor docker container with the
required set of environment variables:

```bash
cat << 'EOF' > castor.conf
IS_MASTER=true
SLAVE_URI=http://localhost:20100
REDIS_HOST=localhost
REDIS_PORT=6379
DB_USER=user
DB_PASSWORD=secret
MINIO_ENDPOINT=http://localhost:9000
EOF
docker run --env-file castor.conf carbynestack/castor-service 
```
