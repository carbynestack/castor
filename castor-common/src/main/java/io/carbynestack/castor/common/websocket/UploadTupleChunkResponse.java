/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.common.websocket;

import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.io.Serializable;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.SerializationUtils;

/**
 * An entity class representing a response provided by the server to a client's {@link TupleChunk}
 * upload request.
 *
 * <p>In case the request failed, the service might provide a detailed error message (see {@link
 * #errorMsg})
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UploadTupleChunkResponse implements Serializable {
  private static final long serialVersionUID = 9192982150705341482L;
  public static final String INVALID_PAYLOAD_EXCEPTION_MSG =
      "Failed deserializing the UploadTupleChunkResponse.";

  /** The id of the recently uploaded {@link TupleChunk} */
  UUID chunkId;
  /**
   * A detailed error description in case the request failed.
   *
   * <p><i>null</i> if the request was successful or no error details were provided
   */
  String errorMsg;
  /** Indicator whether the request was successful or not */
  boolean isSuccess;

  public static UploadTupleChunkResponse success(@NonNull UUID chunkId) {
    return new UploadTupleChunkResponse(chunkId, null, true);
  }

  public static UploadTupleChunkResponse failure(UUID chunkId, String errorMsg) {
    return new UploadTupleChunkResponse(chunkId, errorMsg, false);
  }

  /**
   * Deserializes an {@link UploadTupleChunkResponse}
   *
   * @param payload the serialized data
   * @return the deserialized {@link UploadTupleChunkResponse}
   * @throws CastorClientException in case deserializing the object failed.
   */
  public static UploadTupleChunkResponse fromPayload(byte[] payload) {
    try {
      return SerializationUtils.deserialize(payload);
    } catch (Exception e) {
      throw new CastorClientException(INVALID_PAYLOAD_EXCEPTION_MSG, e);
    }
  }
}
