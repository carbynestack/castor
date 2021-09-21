/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.download;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.concurrent.Cancellable;

/** Created by bks1si on 28.06.17. */
@Slf4j
@RequiredArgsConstructor
public class WaitForReservationCallable implements Callable<Reservation>, Cancellable {

  private boolean hasStarted = false;
  private boolean stop = false;
  private final String reservationId;
  private final ReservationCachingService reservationCachingService;
  private final long retryDelay;

  @Override
  public Reservation call() {
    this.hasStarted = true;
    while (!this.stop || Thread.currentThread().isInterrupted()) {
      try {
        Reservation reservation = reservationCachingService.getReservation(this.reservationId);
        if (reservation != null) {
          if (reservation.getStatus() == ActivationStatus.UNLOCKED) {
            return reservation;
          } else {
            log.debug("Reservation found, but it is still locked: {}", reservation);
          }
        } else {
          // no reservation found
          log.debug("No reservation was found for id {}.", reservationId);
        }
        TimeUnit.MILLISECONDS.sleep(retryDelay);
      } catch (InterruptedException ie) {
        log.debug("Get Reservation Thread interrupted.", ie);
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  @Override
  public boolean cancel() {
    boolean cancelled = this.hasStarted && !this.stop;
    this.stop = cancelled;
    return cancelled;
  }
}
