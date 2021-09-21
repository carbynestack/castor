/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.rest;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleChunk;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This class provides all resource paths and parameter names as exposed and used by the Castor
 * service
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CastorRestApiEndpoints {

  /** Path for intra vcp operations */
  public static final String INTRA_VCP_OPERATIONS_SEGMENT = "/intra-vcp";

  /** Path for secure client up- and download operations */
  public static final String INTER_VCP_OPERATIONS_SEGMENT = "/inter-vcp";

  /** Access {@link Tuple}s */
  public static final String TUPLES_ENDPOINT = "/tuples";

  /** Access {@link TupleChunk}s */
  public static final String TUPLE_CHUNKS_ENDPOINT = "/tuple-chunks";

  /** Upload {@link TupleChunk}s */
  public static final String UPLOAD_TUPLES_ENDPOINT = TUPLES_ENDPOINT + "/upload";

  /** Endpoint to activate a tuple chunk tuples (set status to {@link ActivationStatus#UNLOCKED}) */
  public static final String ACTIVATE_TUPLE_CHUNK_ENDPOINT = TUPLE_CHUNKS_ENDPOINT + "/activate";

  /** Parameter for specifying tuple type */
  public static final String DOWNLOAD_TUPLE_TYPE_PARAMETER = "tupletype";

  /** Parameter for specifying the number of requested items */
  public static final String DOWNLOAD_COUNT_PARAMETER = "count";
  /** Id for the download request */
  public static final String DOWNLOAD_REQUEST_ID_PARAMETER = "reservationId";
  /** Id for the tuple chunk */
  public static final String TUPLE_CHUNK_ID_PARAMETER = "chunkId";

  /** Access telemetry data */
  public static final String TELEMETRY_ENDPOINT = "/telemetry";

  /** Parameter for specifying a custom telemetry aggregation interval in seconds */
  public static final String TELEMETRY_INTERVAL = "interval";

  /**
   * Endpoint to receive and update shared reservations.
   *
   * <p>To be exposed by slave services only.
   */
  public static final String RESERVATION_ENDPOINT = "/reservation";
}
