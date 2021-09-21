/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class DefaultTuplesDownloadService implements TuplesDownloadService {

  public static final String NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG =
      "Operating as master even though service is not declared to be the master";
  public static final String NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG =
      "No released tuple reservation was found for the given request ID #%s";
  public static final String RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG =
      "Reservation does not match expected specification (id: \"%s\", tupleType: \"%s\", count:"
          + " %d): %s";
  public static final String COMMUNICATION_WITH_SLAVES_FAILED_EXCEPTION_MSG =
      "Communication with slaves failed.";
  public static final String FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG =
      "Failed retrieving tuples from database.";
  private final TupleStore tupleStore;
  private final TupleChunkMetaDataStorageService markerStore;
  private final ReservationCachingService reservationCachingService;
  private final CastorSlaveServiceProperties slaveServiceProperties;
  private final Optional<CastorInterVcpClient> castorInterVcpClientOptional;
  private final Optional<DedicatedTransactionService> dedicatedTransactionServiceOptional;

  private ExecutorService executorService = Executors.newCachedThreadPool();
  /**
   * @throws CastorServiceException if reservation was not shared successfully
   * @throws CastorServiceException if communication with slaves failed
   * @throws CastorServiceException if no reservation could be made for the given configuration
   * @throws CastorServiceException if no {@link Reservation} with the given id could be obtained
   *     within a defined timout (see {@link
   *     CastorSlaveServiceProperties#getWaitForReservationTimeout()}).
   */
  @Transactional
  @Override
  public <T extends Tuple<T, F>, F extends Field> TupleList<T, F> getTupleList(
      Class<T> tupleCls, F field, long count, UUID requestId) {
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    String reservationId = requestId + "_" + tupleType;
    Reservation reservation;
    if (castorInterVcpClientOptional.isPresent()
        && dedicatedTransactionServiceOptional.isPresent()) {
      reservation = getOrCreateReservation(reservationId, tupleType, count);
      log.debug("Reservation successfully activated on all slaves.");
    } else {
      reservation = obtainReservation(reservationId);
    }
    TupleList<T, F> result = consumeReservation(tupleCls, field, reservation);
    deleteTupleChunksAndMarkerIfUsed(reservation);
    reservationCachingService.forgetReservation(reservationId);
    return result;
  }

  /**
   * @throws CastorServiceException if reservation was not created successfully
   * @throws CastorServiceException if reservation was not activated successfully
   * @throws CastorServiceException if reservation was not shared successfully
   * @throws CastorServiceException if no reservation could be made for the given configuration
   * @throws CastorServiceException if method is called even tho this castor service is no master
   * @throws CastorServiceException if reservation with given ID is already available but is {@link
   *     ActivationStatus#LOCKED}
   * @throws CastorServiceException if reservation with given ID is already available but does not
   *     match expected configuration
   */
  Reservation getOrCreateReservation(String reservationId, TupleType tupleType, long count) {
    if (!dedicatedTransactionServiceOptional.isPresent()
        || !castorInterVcpClientOptional.isPresent()) {
      throw new CastorServiceException(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG);
    }
    Reservation reservation = reservationCachingService.getReservation(reservationId);
    if (reservation == null) {
      try {
        reservation =
            dedicatedTransactionServiceOptional
                .get()
                .runAsNewTransaction(
                    new CreateReservationSupplier(
                        castorInterVcpClientOptional.get(),
                        reservationCachingService,
                        markerStore,
                        reservationId,
                        tupleType,
                        count));
        reservation =
            dedicatedTransactionServiceOptional
                .get()
                .runAsNewTransaction(
                    new UpdateReservationSupplier(
                        castorInterVcpClientOptional.get(),
                        reservationCachingService,
                        reservation));
      } catch (CastorClientException cce) {
        throw new CastorServiceException(COMMUNICATION_WITH_SLAVES_FAILED_EXCEPTION_MSG, cce);
      }
    } else {
      if (reservation.getStatus() != ActivationStatus.UNLOCKED) {
        throw new CastorServiceException(
            String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId));
      } else if (reservation.getTupleType() != tupleType
          || reservation.getReservations().stream()
                  .mapToLong(ReservationElement::getReservedTuples)
                  .sum()
              != count) {
        throw new CastorServiceException(
            String.format(
                RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG,
                reservationId,
                tupleType,
                count,
                reservation));
      }
    }
    return reservation;
  }

  /**
   * @throws CastorServiceException if no unlocked {@link Reservation} with the given id could be
   *     obtained within a defined timout (see {@link
   *     CastorSlaveServiceProperties#getWaitForReservationTimeout()}).
   */
  Reservation obtainReservation(String reservationId) {
    Reservation reservation = null;
    WaitForReservationCallable waitForReservationCallable =
        new WaitForReservationCallable(
            reservationId, reservationCachingService, slaveServiceProperties.getRetryDelay());
    try {
      reservation =
          executorService
              .submit(waitForReservationCallable)
              .get(slaveServiceProperties.getWaitForReservationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      waitForReservationCallable.cancel();
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      waitForReservationCallable.cancel();
    }
    if (reservation == null) {
      throw new CastorServiceException(
          String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId));
    }
    return reservation;
  }

  private <T extends Tuple<T, F>, F extends Field> TupleList<T, F> consumeReservation(
      Class<T> tupleCls, F field, Reservation reservation) {
    TupleList<T, F> tuples = new TupleList<>(tupleCls, field);
    tuples.addAll(
        reservation.getReservations().stream()
            .flatMap(
                reservationElement -> downloadTuples(tupleCls, field, reservationElement).stream())
            .collect(Collectors.toList()));
    return tuples;
  }

  /** @throws CastorServiceException if tuples cannot be retrieved from database */
  private <T extends Tuple<T, F>, F extends Field> TupleList<T, F> downloadTuples(
      Class<T> tupleCls, F field, ReservationElement reservationElement) {
    UUID tupleChunkId = reservationElement.getTupleChunkId();
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    final long offset = reservationElement.getStartIndex() * tupleType.getTupleSize();
    final long length = reservationElement.getReservedTuples() * tupleType.getTupleSize();
    return tupleStore.downloadTuples(tupleCls, field, tupleChunkId, offset, length);
  }

  /** @throws CastorServiceException if metadata cannot be updated */
  private void deleteTupleChunksAndMarkerIfUsed(Reservation reservation) {
    for (ReservationElement reservationElement : reservation.getReservations()) {
      UUID tupleChunkId = reservationElement.getTupleChunkId();
      TupleChunkMetaDataEntity tupleChunkData =
          markerStore.updateConsumptionForTupleChunkData(
              tupleChunkId, reservationElement.getReservedTuples());
      if (tupleChunkData.getConsumedMarker() == tupleChunkData.getNumberOfTuples()) {
        markerStore.forgetTupleChunkData(tupleChunkId);
        tupleStore.deleteTupleChunk(tupleChunkId);
      }
    }
  }
}
