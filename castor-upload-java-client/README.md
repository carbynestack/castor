# Castor Upload Client - A Java Library to upload Tuples to Castor

This Java library provides a client to upload tuples to a
[Castor](../castor-service) service. The client therefore establishes a
WebSocket connection to the Castor Service and uploads the individual shares
provided. Within _Castor_, tuples are managed in so called tuple chunks which
allows for batch upload and efficient tuple management. In order to ensure that
related shares are available on all parties of the Virtual Cloud before being
consumed on a single party, newly uploaded chunks must be cleared for
consumption first. More precisely, newly stored tuple chunks will not be used
before being explicitly activated.

> :warning: **WARNING**: When uploading tuples to multiple services, each
> service must be provided with its own, individual shares of the tuples. It is
> mandatory that only shares of the same tuples are submitted with the same
> identifier when uploading a chunk. For example, computations will produce
> invalid results in case services hold unrelated tuple shares for the same id.

## Provided Clients

### UploadClient

This client can be used to upload chunks of tuple shares to a _Castor_ service
and to activate them for consumption. Uploading will not activate a chunk
automatically. Therefore, it is required to explicitly activate the chunk on all
parties only if the upload request was confirmed by all parties.

The interface is described in
`io.carbynestack.castor.client.upload.CastorUploadClient` and the default
implementation is
`io.carbynestack.castor.client.upload,DefaultCastorUploadClient`.

#### Usage Example

The following example shows a class that is instantiated with the address of a
_Castor Service_ endpoint. `uploadTuples` shows how to read tuple shares from a
file and upload the data as a tuple chunk to a provider within a _Carbyne Stack_
Virtual Cluster. On successful upload, the just uploaded tuple chunk will be
activated for consumption.

```java
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.ConnectionFailedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

import static org.apache.commons.io.IOUtils.toByteArray;

public class Example {

    private static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10000;
    private CastorUploadClient castorUploadClient;

    public Example(String serviceAddress) {
        this.castorUploadClient =
                DefaultCastorUploadClient.builder(serviceAddress).build();
    }
    
    public void uploadTuples(File tupleFile, TupleType tupleType)
            throws IOException {
        UUID chunkId = UUID.randomUUID();
        TupleChunk tupleChunk;
        try (FileInputStream fileInputStream = new FileInputStream(tupleFile)) {
            tupleChunk = TupleChunk.of(tupleType.getTupleCls(),
                    tupleType.getField(),
                    chunkId,
                    toByteArray(fileInputStream));
        }
        castorUploadClient.connectWebSocket(DEFAULT_CONNECTION_TIMEOUT_MILLIS);
        if (castorUploadClient.uploadTupleChunk(tupleChunk)) {
            castorUploadClient.activateTupleChunk(chunkId);
        }
    }

}
```

## Getting Started

Castor uses Maven for dependency and build management. You can add
_castor-upload-java-client_ to your project by declaring the following maven
dependency in your `pom.xml`:

### Maven

```xml

<dependency>
    <groupId>io.carbynestack</groupId>
    <artifactId>castor-upload-java-client</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```

### Building from Source

The _castor-upload-java-client_ library can be build and installed in the local
maven repository using:

```bash
../mvnw install
```
