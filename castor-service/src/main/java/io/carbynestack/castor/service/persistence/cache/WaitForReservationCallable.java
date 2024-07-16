/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.annotation.Timed;
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
  private final TupleType tupleType;
  private final long numberOfTuples;
  private final ReservationCachingService reservationCachingService;
  private final long retryDelay;

  /**
   * @return the reservations matching the given criteria
   * @throws CastorServiceException if reservation with given ID is available but does not match
   *     given criteria
   */
  @Override
  @Timed
  public Reservation call() {
    this.hasStarted = true;
    while (!this.stop || Thread.currentThread().isInterrupted()) {
      try {
        Reservation reservation =
            reservationCachingService.getUnlockedReservation(
                this.reservationId, tupleType, numberOfTuples);
        if (reservation != null) {
          return reservation;
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
