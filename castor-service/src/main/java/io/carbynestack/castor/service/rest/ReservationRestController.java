/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.rest;

import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequiredArgsConstructor(onConstructor_ = @Autowired)
@RequestMapping(path = INTER_VCP_OPERATIONS_SEGMENT + RESERVATION_ENDPOINT)
public class ReservationRestController {
  private final ReservationCachingService reservationCachingService;

  @PostMapping
  public ResponseEntity<String> reserveTuples(@RequestBody Reservation reservation) {
    reservation.setStatus(ActivationStatus.LOCKED);
    reservationCachingService.keepAndApplyReservation(reservation);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @PutMapping(path = "/{" + DOWNLOAD_REQUEST_ID_PARAMETER + "}")
  public ResponseEntity<String> updateReservationStatus(
      @PathVariable String reservationId, @RequestBody ActivationStatus status) {
    log.debug("Received update for reservation #{} to status {}", reservationId, status);
    reservationCachingService.updateReservation(reservationId, status);
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
