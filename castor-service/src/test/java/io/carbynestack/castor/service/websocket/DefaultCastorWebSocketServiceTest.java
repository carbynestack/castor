/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.websocket;

import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT;
import static io.carbynestack.castor.service.websocket.DefaultCastorWebSocketService.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.MultiplicationTriple;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCastorWebSocketServiceTest {

  @Mock private TupleStore tupleStoreMock;

  @Mock private SimpMessagingTemplate messagingTemplateMock;

  @Mock private TupleChunkFragmentStorageService fragmentStorageService;

  @InjectMocks private DefaultCastorWebSocketService castorWebSocketService;

  @Test
  public void givenInvalidPayload_whenUploadTupleChunk_thenThrowCastorClientException() {
    byte[] invalidPayload = new byte[0];

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> castorWebSocketService.uploadTupleChunk(null, invalidPayload));

    assertEquals(UNSUPPORTED_PAYLOAD_TYPE_ERROR_MSG, actualCce.getMessage());
    verify(messagingTemplateMock)
        .convertAndSend(
            RESPONSE_QUEUE_ENDPOINT,
            SerializationUtils.serialize(
                UploadTupleChunkResponse.failure(null, UNSUPPORTED_PAYLOAD_TYPE_ERROR_MSG)));
  }

  @Test
  public void givenChunkHasNoTuples_whenUploadTupleChunk_thenThrowCastorClientException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleChunk tupleChunk = TupleChunk.of(MultiplicationTriple.class, GFP, chunkId, new byte[0]);
    byte[] payload = SerializationUtils.serialize(tupleChunk);

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> castorWebSocketService.uploadTupleChunk(null, payload));

    assertEquals(CHUNK_MUST_NOT_BE_EMPTY_EXCEPTION_MSG, actualCce.getMessage());
    verify(messagingTemplateMock)
        .convertAndSend(
            RESPONSE_QUEUE_ENDPOINT,
            SerializationUtils.serialize(
                UploadTupleChunkResponse.failure(chunkId, CHUNK_MUST_NOT_BE_EMPTY_EXCEPTION_MSG)));
  }

  @Test
  public void
      givenWritingChunkToDatabaseFails_whenUploadTupleChunk_thenThrowCastorServiceException() {
    CastorServiceException expectedException = new CastorServiceException("expected");
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    byte[] tupleData = RandomUtils.nextBytes(tupleType.getTupleSize());
    TupleChunk tupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    byte[] payload = SerializationUtils.serialize(tupleChunk);

    doThrow(expectedException).when(tupleStoreMock).save(tupleChunk);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> castorWebSocketService.uploadTupleChunk(null, payload));

    assertEquals(FAILED_WRITING_TO_DATABASE_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
    verify(messagingTemplateMock)
        .convertAndSend(
            RESPONSE_QUEUE_ENDPOINT,
            SerializationUtils.serialize(
                UploadTupleChunkResponse.failure(
                    chunkId, FAILED_WRITING_TO_DATABASE_EXCEPTION_MSG)));
  }

  @Test
  public void givenSuccessfulRequest_whenUploadChunk_thenSendSuccess() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    byte[] tupleData = RandomUtils.nextBytes(tupleType.getTupleSize());
    TupleChunk tupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    byte[] payload = SerializationUtils.serialize(tupleChunk);

    castorWebSocketService.uploadTupleChunk(null, payload);

    verify(tupleStoreMock).save(tupleChunk);
    verify(fragmentStorageService)
        .keep(TupleChunkFragmentEntity.of(chunkId, tupleType, 0, tupleChunk.getNumberOfTuples()));

    verify(messagingTemplateMock, times(1))
        .convertAndSend(
            RESPONSE_QUEUE_ENDPOINT,
            SerializationUtils.serialize(UploadTupleChunkResponse.success(chunkId)));
  }
}
