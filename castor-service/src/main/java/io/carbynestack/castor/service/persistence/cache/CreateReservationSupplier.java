/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import io.micrometer.core.annotation.Timed;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** see {@link #get()} */
@Slf4j
@AllArgsConstructor
public final class CreateReservationSupplier implements Supplier<Reservation> {
  public static final String INSUFFICIENT_TUPLES_EXCEPTION_MSG =
      "Insufficient Tuples of type %s available (%s out of %s).";
  public static final String SHARING_RESERVATION_FAILED_EXCEPTION_MSG =
      "Sharing reservation with slave services failed.";
  public static final String FAILED_RESERVE_TUPLES_EXCEPTION_MSG = "Failed to reserve the tuples.";
  public static final String FAILED_FETCH_AVAILABLE_FRAGMENT_EXCEPTION_MSG =
      "Unable to locate available tuples.";
  final CastorInterVcpClient castorInterVcpClient;
  final ReservationCachingService reservationCache;
  final TupleChunkFragmentStorageService fragmentStorageService;
  final String reservationId;
  final TupleType tupleType;
  final long count;

  /**
   * @throws CastorServiceException if not enough tuples of the given type are available
   * @throws CastorServiceException if reserving the requested amount of tuples failed, although
   *     there are enough tuples available
   * @throws CastorClientException if communication with slaves failed while sharing the reservation
   * @throws CastorServiceException if reservation was not shared successfully
   */
  @Override
  public Reservation get() {
    List<ReservationElement> reservationElements = composeElements(tupleType, count, reservationId);
    log.debug("Reservation composed.");
    Reservation reservation = new Reservation(reservationId, tupleType, reservationElements);
    if (!castorInterVcpClient.shareReservation(reservation)) {
      throw new CastorServiceException(SHARING_RESERVATION_FAILED_EXCEPTION_MSG);
    }
    log.debug("Reservation successfully shared with all slaves.");
    return reservation;
  }

  /**
   * @throws CastorServiceException if not enough tuples of the given type are available
   * @throws CastorServiceException if reserving the requested amount of tuples failed, although
   *     there are enough tuples available
   */
  private List<ReservationElement> composeElements(
      TupleType tupleType, long numberOfTuples, String reservationId) {
    long availableTuples = fragmentStorageService.getAvailableTuples(tupleType);
    if (availableTuples < numberOfTuples) {
      throw new CastorServiceException(
          String.format(
              INSUFFICIENT_TUPLES_EXCEPTION_MSG, tupleType, availableTuples, numberOfTuples));
    }
    List<ReservationElement> reservationElements = new ArrayList<>();
    long stillToReserve = numberOfTuples;
    while (stillToReserve > 0) {
      try {
        TupleChunkFragmentEntity availableFragment =
            fragmentStorageService
                .findAvailableFragmentWithTupleType(tupleType)
                .orElseThrow(
                    () ->
                        new CastorServiceException(FAILED_FETCH_AVAILABLE_FRAGMENT_EXCEPTION_MSG));
        long tuplesInFragment = availableFragment.getEndIndex() - availableFragment.getStartIndex();
        if (tuplesInFragment > stillToReserve) {
          availableFragment =
              fragmentStorageService.splitAt(
                  availableFragment, availableFragment.getStartIndex() + stillToReserve);
        }
        availableFragment.setReservationId(reservationId);
        fragmentStorageService.update(availableFragment);
        long tuplesTaken = Math.min(tuplesInFragment, stillToReserve);
        stillToReserve -= tuplesTaken;
        reservationElements.add(
            new ReservationElement(
                availableFragment.getTupleChunkId(),
                tuplesTaken,
                availableFragment.getStartIndex()));
      } catch (Exception e) {
        throw new CastorServiceException(FAILED_RESERVE_TUPLES_EXCEPTION_MSG, e);
      }
    }
    log.debug("Composed reservation of {} {}: {}.", numberOfTuples, tupleType, reservationElements);
    return reservationElements;
  }
}
