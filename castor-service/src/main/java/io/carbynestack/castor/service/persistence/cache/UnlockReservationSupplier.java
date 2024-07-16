/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.function.Supplier;

import io.micrometer.core.annotation.Timed;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
final class UnlockReservationSupplier implements Supplier<Reservation> {
  final CastorInterVcpClient castorInterVcpClient;
  final ReservationCachingService reservationCachingService;
  final Reservation reservation;

  /**
   * @throws CastorClientException if communication with slaves failed either while activating the
   *     reservation
   * @throws CastorServiceException if no Reservation is associated with the given reservation's ID
   */
  @Override
  @Timed
  public Reservation get() {
    log.debug("updating reservation {}", reservation.getReservationId());
    reservation.setStatus(ActivationStatus.UNLOCKED);
    castorInterVcpClient.updateReservationStatus(
        reservation.getReservationId(), reservation.getStatus());
    log.debug("update distributed");
    reservationCachingService.updateReservation(
        reservation.getReservationId(), reservation.getStatus());
    log.debug("update applied locally");
    return reservation;
  }
}
