/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.UUID;

/**
 * Client interface for all MPC-Cluster internal Service-to-Castor-Service operations used to
 * download {@link TupleList}s or to download telemetry data containing tuple consumption rate and
 * number of available tuples from castor.<br>
 * Consumption rate is always calculated as consumption per second for a given interval (the last n
 * seconds)..
 */
public interface CastorIntraVcpClient {

  /**
   * Retrieves a requested amount of {@link Tuple}s from the Castor Service using a pre-defined
   * request id.
   *
   * @param requestId Unique id to identify related requests across multiple castor services.
   * @param tupleType The tuple type to be downloaded
   * @param count Requested number of {@link Tuple}s.
   * @return A {@link TupleList} containing the requested tuples.
   * @throws CastorClientException if composing the request tuples URI failed
   * @throws CastorClientException if download the tuples from the service failed
   */
  TupleList downloadTupleShares(UUID requestId, TupleType tupleType, long count);

  /**
   * Retrieves latest telemetry data with an interval preconfigured in castor.
   *
   * @return Telemetry data for each tuple type
   * @throws CastorClientException if composing the request telemetry URI failed
   * @throws CastorClientException if retrieving the telemetry metrics failed
   */
  TelemetryData getTelemetryData();

  /**
   * Retrieves latest telemetry data with a custom interval.
   *
   * @param interval Time period in seconds.
   * @return Telemetry data for each tuple type
   * @throws CastorClientException if composing the request telemetry URI failed
   * @throws CastorClientException if retrieving the telemetry metrics failed
   */
  TelemetryData getTelemetryData(long interval);
}
