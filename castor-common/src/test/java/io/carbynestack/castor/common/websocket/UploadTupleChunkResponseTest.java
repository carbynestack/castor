/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.common.websocket;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;

public class UploadTupleChunkResponseTest {

  @Test
  public void givenResponseWithSuccess_whenSerializationRoundTrip_thenRecoverActualResponse() {
    UUID expectedChunkId = UUID.randomUUID();
    byte[] request =
        SerializationUtils.serialize(UploadTupleChunkResponse.success(expectedChunkId));
    UploadTupleChunkResponse actualUploadTupleChunkResponse =
        UploadTupleChunkResponse.fromPayload(request);
    assertEquals(expectedChunkId, actualUploadTupleChunkResponse.getChunkId());
    assertTrue(actualUploadTupleChunkResponse.isSuccess());
    assertNull(actualUploadTupleChunkResponse.getErrorMsg());
  }

  @Test
  public void givenInvalidPayload_whenDeserialize_thenThrowCastorClientException() {
    UUID chunkId = UUID.randomUUID();
    byte[] payload = SerializationUtils.serialize(UploadTupleChunkResponse.success(chunkId));
    byte[] invalidPayload = ArrayUtils.subarray(payload, 0, payload.length - 1);
    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> UploadTupleChunkResponse.fromPayload(invalidPayload));
    assertEquals(UploadTupleChunkResponse.INVALID_PAYLOAD_EXCEPTION_MSG, actualCce.getMessage());
  }

  @Test
  public void givenResponseWithFailure_whenSerializationRoundTrip_thenRecoverActualResponse() {
    UUID expectedChunkId = UUID.randomUUID();
    String expectedErrorMsg = "Expected message";
    byte[] request =
        SerializationUtils.serialize(
            UploadTupleChunkResponse.failure(expectedChunkId, expectedErrorMsg));
    UploadTupleChunkResponse actualUploadTupleChunkResponse =
        UploadTupleChunkResponse.fromPayload(request);
    assertEquals(expectedChunkId, actualUploadTupleChunkResponse.getChunkId());
    assertFalse(actualUploadTupleChunkResponse.isSuccess());
    assertEquals(expectedErrorMsg, actualUploadTupleChunkResponse.getErrorMsg());
  }
}
