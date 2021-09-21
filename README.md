# Carbyne Stack Castor Tuple Store

[![codecov](https://codecov.io/gh/carbynestack/castor/branch/master/graph/badge.svg?token=JWqyS02Uok)](https://codecov.io/gh/carbynestack/castor)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/b13366556ed545189445c9107d7fded7)](https://www.codacy.com?utm_source=github.com&utm_medium=referral&utm_content=carbynestack/castor&utm_campaign=Badge_Grade)
[![Known Vulnerabilities](https://snyk.io/test/github/carbynestack/castor/badge.svg)](https://snyk.io/test/github/carbynestack/castor)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

Castor is an open source storage service for cryptographic material used in
Secure Multiparty Computation, so called _tuples_, and part of the
[Carbyne Stack](https://github.com/carbynestack) platform.

> **DISCLAIMER**: Carbyne Stack Castor is *alpha* software. The software is not
> ready for production use. It has neither been developed nor tested for a
> specific use case.

Please have a look at the underlying modules for more information on how to run
a Castor service and how to interact with it using the provided Java clients:

- [Castor Common](castor-common) - A shared library of commonly used
  functionality.
- [Castor Service](castor-service) - The microservice implementing the backend
  storage facilities for tuples.
- [Castor Java Client](castor-java-client) - A Java client library to interact
  with a Castor service over its REST API. The module provides client
  implementations to communicate
  - with the Castor service within a Virtual Cloud Provider.
  - across Castor services participating in a Virtual Cloud.
- [Castor Java Upload Client](castor-upload-java-client) - A Java client used to
  upload pre-generated tuples using Castor's WebSocket interface.

> :bulb: **NOTE**\
> _Castor_ is only used to manage tuples in a Carbyne Stack
> Virtual Cloud and does not provide any functionality for generating the tuples
> themselves.

## Namesake

_Castor_, a genus name of the beaver. The service name is derived from Beaver
triples (proposed by Donald Rozinak Beaver), stored as one specialized type of
tuples in the castor service.

## License

Carbyne Stack *Castor Tuple Store* is open-sourced under the Apache License 2.0.
See the [LICENSE](LICENSE) file for details.

### 3rd Party Licenses

For information on how license obligations for 3rd party OSS dependencies are
fulfilled see the [README](https://github.com/carbynestack/carbynestack) file of
the Carbyne Stack repository.

## Contributing

Please see the Carbyne Stack
[Contributor's Guide](https://github.com/carbynestack/carbynestack/blob/master/CONTRIBUTING.md)
.
