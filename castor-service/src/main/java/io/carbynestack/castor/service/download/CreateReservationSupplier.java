/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static java.lang.Math.min;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** see {@link #get()} */
@Slf4j
@AllArgsConstructor
final class CreateReservationSupplier implements Supplier<Reservation> {
  public static final String INSUFFICIENT_TUPLES_EXCEPTION_MSG =
      "Insufficient Tuples of type %s available (%s out of %s)";
  public static final String SHARING_RESERVATION_FAILED_EXCEPTION_MSG =
      "Sharing reservation with slave services failed.";
  public static final String FAILED_RESERVE_AMOUNT_TUPLES_EXCEPTION_MSG =
      "Failed to reserve the requested amount of tuples (%s) although available  %d out of %d"
          + " available.";
  final CastorInterVcpClient castorInterVcpClient;
  final ReservationCachingService reservationCache;
  final TupleChunkMetaDataStorageService tupleChunkMetaDataStorageService;
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
    List<ReservationElement> reservationElements = composeElements(tupleType, count);
    log.debug("Reservation composed.");
    Reservation reservation = new Reservation(reservationId, tupleType, reservationElements);
    reservationCache.keepReservation(reservation);
    log.debug("Reservation persisted.");
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
  private List<ReservationElement> composeElements(TupleType tupleType, long numberOfTuples) {
    long availableTuples = tupleChunkMetaDataStorageService.getAvailableTuples(tupleType);
    if (availableTuples < numberOfTuples) {
      throw new CastorServiceException(
          String.format(
              INSUFFICIENT_TUPLES_EXCEPTION_MSG, tupleType, availableTuples, numberOfTuples));
    }
    List<ReservationElement> reservationElements = new ArrayList<>();
    long stillToReserve = numberOfTuples;

    List<TupleChunkMetaDataEntity> tupleChunkDataList =
        tupleChunkMetaDataStorageService.getTupleChunkData(tupleType);
    for (TupleChunkMetaDataEntity tupleChunkData : tupleChunkDataList) {
      long tuplesToRead =
          min(
              tupleChunkData.getNumberOfTuples() - tupleChunkData.getReservedMarker(),
              stillToReserve);
      reservationElements.add(
          new ReservationElement(
              tupleChunkData.getTupleChunkId(), tuplesToRead, tupleChunkData.getReservedMarker()));
      stillToReserve -= tuplesToRead;
      if (stillToReserve == 0) {
        break;
      }
    }
    if (stillToReserve > 0) {
      throw new CastorServiceException(
          String.format(
              FAILED_RESERVE_AMOUNT_TUPLES_EXCEPTION_MSG,
              tupleType,
              (numberOfTuples - stillToReserve),
              numberOfTuples));
    }
    log.debug(
        "Preparing reservation of {} {}: {}.", numberOfTuples, tupleType, reservationElements);
    return reservationElements;
  }
}
