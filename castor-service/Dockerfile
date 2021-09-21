#
# Copyright (c) 2021 - for information on the respective copyright owner
# see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
#
# SPDX-License-Identifier: Apache-2.0
#
ARG BASE_IMAGE="ghcr.io/carbynestack/openjdk:jre8-20210827"

FROM ${BASE_IMAGE}

ARG RELEASE_PAGE="https://github.com/carbynestack/castor/releases"

ENV GROUP_ID=1000 \
    GROUP_NAME="cs" \
    USER_NAME="castor" \
    USER_ID=1000 \
    USER_HOME="/castor"

COPY healthcheck.sh /usr/local/bin/
RUN chmod 755 /usr/local/bin/healthcheck.sh

RUN groupadd -g ${GROUP_ID} ${GROUP_NAME} && \
    useradd -m -d ${USER_HOME} -s /usr/sbin/nologin -g ${GROUP_ID} -u ${USER_ID} ${USER_NAME}

WORKDIR ${USER_HOME}

ADD target/castor-service*-exec.jar castor-service.jar
ADD target/classes/3RD-PARTY-LICENSES /3RD-PARTY-LICENSES
RUN chown -R ${USER_NAME}:${GROUP_ID} ./* /3RD-PARTY-LICENSES/ && \
  chmod 740 castor-service.jar && \
  chmod -R ugo+rX-w /3RD-PARTY-LICENSES

RUN printf "\n## Castor Service\n\
General information about third-party software components and their licenses, \
which are distributed with _Castor_, can be found in the [SBOM](./sbom.xml). \
Further details are available in the subfolders of the respective components or \
can be downloaded from the [Castor Release Page](%s).\n" "${RELEASE_PAGE}"\
  >> /3RD-PARTY-LICENSES/disclosure.md

EXPOSE 10100

USER ${USER_NAME}

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","castor-service.jar"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=180s \
    CMD /usr/local/bin/healthcheck.sh
