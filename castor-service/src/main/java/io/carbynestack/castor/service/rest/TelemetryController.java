/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.rest;

import static io.carbynestack.castor.common.entities.TelemetryData.DEFAULT_REQUEST_INTERVAL;
import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.*;

import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.download.TelemetryService;
import java.time.Duration;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = INTRA_VCP_OPERATIONS_SEGMENT + TELEMETRY_ENDPOINT)
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TelemetryController {
  private final TelemetryService telemetryService;

  /**
   * Retrieves the telemetry data (consumption per second and available tuples) for all tuple types.
   *
   * <p>Please be aware that telemetry data will be removed after a fixed time automatically (see
   * {@link CastorCacheProperties#getTelemetryTtl()}). Setting the requestInterval to any value
   * larger than the configured ttl is therefore of no use.
   *
   * @param requestInterval Optional parameter to define the aggregation interval in seconds.
   * @throws IllegalArgumentException if requestInterval is less than 1.
   */
  @GetMapping
  public ResponseEntity<TelemetryData> getTelemetryData(
      @RequestParam(value = TELEMETRY_INTERVAL, required = false) Long requestInterval) {
    if (requestInterval != null && requestInterval <= 0) {
      throw new IllegalArgumentException(
          String.format("%s must be larger than 0", TELEMETRY_INTERVAL));
    }
    Duration interval =
        requestInterval != null ? Duration.ofSeconds(requestInterval) : DEFAULT_REQUEST_INTERVAL;
    return new ResponseEntity<>(
        telemetryService.getTelemetryDataForInterval(interval), HttpStatus.OK);
  }
}
