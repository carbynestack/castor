/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.ConnectionFailedException;
import java.util.UUID;

/** This client is used to upload {@link TupleChunk}s to a single castor service. */
public interface CastorUploadClient {
  /** Default connection timeout for websocket connections to a castor service in milliseconds. */
  long DEFAULT_CONNECTION_TIMEOUT = 60000L;

  /**
   * Uploads a {@link TupleChunk} to castor using a WebSocket connection and wait until for upload
   * to complete or the {@link CastorUploadClient#DEFAULT_CONNECTION_TIMEOUT} to be elapsed.
   *
   * @param tupleChunk The {@link TupleChunk} to upload
   * @return <CODE>true</CODE> when upload was successful or <CODE>false</CODE> if not
   * @throws IllegalArgumentException if number of tuples in the given chunk exceed the maximum
   *     number of allowed tuples per chunk
   * @throws ConnectionFailedException if the websocket connection has not been established yet
   */
  boolean uploadTupleChunk(TupleChunk tupleChunk) throws CastorClientException;

  /**
   * Uploads a {@link TupleChunk} to castor using a WebSocket connection and wait until for upload
   * to complete or a given timeout to be elapsed.
   *
   * @param tupleChunk The {@link TupleChunk} to upload
   * @param timeout the maximum time in milliseconds to wait for the {@link TupleChunk }to be
   *     uploaded
   * @return <CODE>true</CODE> when upload was successful or <CODE>false</CODE> if not
   * @throws IllegalArgumentException if number of tuples in the given chunk exceed the maximum
   *     number of allowed tuples per chunk
   * @throws ConnectionFailedException if the websocket connection has not been established yet
   */
  boolean uploadTupleChunk(TupleChunk tupleChunk, long timeout) throws CastorClientException;

  /**
   * Activates a {@link TupleChunk} by setting its linked {@link ActivationStatus} to {@link
   * ActivationStatus#UNLOCKED}.
   *
   * @param tupleChunkId Unique identifier of the {@link TupleChunk} that should be activated.
   * @throws CastorClientException if the communication with the CastorService failed.
   */
  void activateTupleChunk(UUID tupleChunkId);

  /**
   * Establishes WebSocket connection to all defined endpoints.
   *
   * @param timeout Time to wait for all connections being established in Milliseconds.
   * @throws CastorClientException If the connection cannot be established in time.
   */
  void connectWebSocket(long timeout);

  /** Terminates the active WebSocket connection and the reconnect scheduler. */
  void disconnectWebSocket();
}
