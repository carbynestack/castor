/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.websocket;

import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.UPLOAD_TUPLES_ENDPOINT;

import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Slf4j
@Controller
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class DefaultCastorWebSocketService implements CastorWebSocketService {
  public static final String UNSUPPORTED_PAYLOAD_TYPE_ERROR_MSG = "Unsupported payload type.";
  public static final String CHUNK_MUST_NOT_BE_EMPTY_EXCEPTION_MSG =
      "Chunk must contain at least one tuple.";
  public static final String FAILED_WRITING_TO_DATABASE_EXCEPTION_MSG =
      "Failed writing tuple chunk to database.";
  private final SimpMessagingTemplate template;
  private final TupleStore tupleStore;
  private final TupleChunkFragmentStorageService fragmentStorageService;

  @Override
  @MessageMapping(UPLOAD_TUPLES_ENDPOINT)
  /**
   * @throws CastorClientException if the given payload cannot be cast deserialized as {@link
   *     TupleChunk}
   * @throws CastorClientException if the {@link TupleChunk}'s data is empty or null
   * @throws CastorServiceException if writing the received {@link TupleChunk} to the database
   *     failed
   */
  public void uploadTupleChunk(SimpMessageHeaderAccessor headerAccessor, byte[] payload) {
    log.debug("Received payload...");
    TupleChunk tupleChunk;
    try {
      tupleChunk = SerializationUtils.deserialize(payload);
    } catch (SerializationException | ClassCastException e) {
      sendException(null, UNSUPPORTED_PAYLOAD_TYPE_ERROR_MSG);
      throw new CastorClientException(UNSUPPORTED_PAYLOAD_TYPE_ERROR_MSG);
    }
    if (ArrayUtils.isEmpty(tupleChunk.getTuples())) {
      sendException(tupleChunk.getChunkId(), CHUNK_MUST_NOT_BE_EMPTY_EXCEPTION_MSG);
      throw new CastorClientException(CHUNK_MUST_NOT_BE_EMPTY_EXCEPTION_MSG);
    }

    try {
      tupleStore.save(tupleChunk);
      log.debug("Saved tuple chunk #{}.", tupleChunk.getChunkId());
      fragmentStorageService.keep(
          TupleChunkFragmentEntity.of(
              tupleChunk.getChunkId(),
              tupleChunk.getTupleType(),
              0,
              tupleChunk.getNumberOfTuples()));
      log.debug("Saved fragment for #{}.", tupleChunk.getChunkId());
      UploadTupleChunkResponse response = UploadTupleChunkResponse.success(tupleChunk.getChunkId());
      template.convertAndSend(
          CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT,
          SerializationUtils.serialize(response));
    } catch (Exception e) {
      log.error("Exception occurred while writing TupleChunk to database.", e);
      sendException(tupleChunk.getChunkId(), FAILED_WRITING_TO_DATABASE_EXCEPTION_MSG);
      throw new CastorServiceException(FAILED_WRITING_TO_DATABASE_EXCEPTION_MSG, e);
    }
  }

  private void sendException(UUID uuid, String errorMessage) {
    template.convertAndSend(
        CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT,
        SerializationUtils.serialize(UploadTupleChunkResponse.failure(uuid, errorMessage)));
  }
}
