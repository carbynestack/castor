/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.websocket;

import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT;
import static io.carbynestack.castor.service.websocket.DefaultCastorWebSocketService.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.MultiplicationTriple;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@ExtendWith(MockitoExtension.class)
class DefaultCastorWebSocketServiceTest {

  @Mock private TupleStore tupleStoreMock;

  @Mock private SimpMessagingTemplate messagingTemplateMock;

  @Mock private TupleChunkFragmentStorageService fragmentStorageService;

  @Mock private CastorServiceProperties servicePropertiesMock;

  @InjectMocks private DefaultCastorWebSocketService castorWebSocketService;

  @Test
  void givenInvalidPayload_whenUploadTupleChunk_thenThrowCastorClientException() {
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
  void givenChunkHasNoTuples_whenUploadTupleChunk_thenThrowCastorClientException() {
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
  void givenWritingChunkToDatabaseFails_whenUploadTupleChunk_thenThrowCastorServiceException() {
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
  void givenSuccessfulRequest_whenUploadChunk_thenSendSuccess() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    byte[] tupleData = RandomUtils.nextBytes(tupleType.getTupleSize());
    TupleChunk tupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    byte[] payload = SerializationUtils.serialize(tupleChunk);
    int initialFragmentSize = 7;

    when(servicePropertiesMock.getInitialFragmentSize()).thenReturn(initialFragmentSize);

    castorWebSocketService.uploadTupleChunk(null, payload);

    verify(tupleStoreMock).save(tupleChunk);
    verify(fragmentStorageService)
        .keep(
            Collections.singletonList(
                TupleChunkFragmentEntity.of(
                    chunkId,
                    tupleType,
                    0,
                    tupleChunk.getNumberOfTuples(),
                    ActivationStatus.LOCKED,
                    null,
                    false)));

    verify(messagingTemplateMock, times(1))
        .convertAndSend(
            RESPONSE_QUEUE_ENDPOINT,
            SerializationUtils.serialize(UploadTupleChunkResponse.success(chunkId)));
  }

  @Test
  void givenChunkWithZeroTuples_whenGenerateFragments_thenReturnEmptyList() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    TupleChunk emptyTupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, new byte[0]);
    // doReturn(1000).when(servicePropertiesMock).getInitialFragmentSize();

    assertEquals(
        castorWebSocketService.generateFragmentsForChunk(emptyTupleChunk), Collections.emptyList());
  }

  @Test
  void givenChunkWithMultipleTuples_whenGenerateFragments_thenGenerateFragmentsAccordingly() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    int initialFragmentSize = 7;
    int numberOfTuples = initialFragmentSize * 2 - 1;
    byte[] tupleData = RandomUtils.nextBytes(tupleType.getTupleSize() * numberOfTuples);
    TupleChunk tupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    List<TupleChunkFragmentEntity> expectedFragments =
        Arrays.asList(
            TupleChunkFragmentEntity.of(chunkId, tupleType, 0, initialFragmentSize),
            TupleChunkFragmentEntity.of(
                chunkId,
                tupleType,
                initialFragmentSize,
                numberOfTuples,
                ActivationStatus.LOCKED,
                null,
                false));

    when(servicePropertiesMock.getInitialFragmentSize()).thenReturn(initialFragmentSize);

    assertEquals(expectedFragments, castorWebSocketService.generateFragmentsForChunk(tupleChunk));
  }
}
