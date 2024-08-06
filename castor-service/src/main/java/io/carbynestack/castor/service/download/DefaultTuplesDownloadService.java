/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class DefaultTuplesDownloadService implements TuplesDownloadService {
  public static final String FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG =
      "Failed retrieving tuples from database.";

  private final TupleStore tupleStore;
  private final TupleChunkFragmentStorageService fragmentStorageService;
  private final ReservationCachingService reservationCachingService;
  private final CastorServiceProperties castorServiceProperties;

  @Autowired
  public DefaultTuplesDownloadService(
      TupleStore tupleStore,
      TupleChunkFragmentStorageService fragmentStorageService,
      ReservationCachingService reservationCachingService,
      CastorServiceProperties castorServiceProperties) {
    this.tupleStore = tupleStore;
    this.fragmentStorageService = fragmentStorageService;
    this.reservationCachingService = reservationCachingService;
    this.castorServiceProperties = castorServiceProperties;
  }

  /**
   * @throws CastorServiceException if reservation was not shared successfully
   * @throws CastorServiceException if communication with slaves failed
   * @throws CastorServiceException if no reservation could be made for the given configuration
   * @throws CastorServiceException if no {@link Reservation} with the given id could be obtained
   *     within a defined timout (see {@link
   *     CastorSlaveServiceProperties#getWaitForReservationTimeout()}).
   * @throws CastorServiceException if reservation with given ID is already available but does not
   *     match given criteria
   * @throws CastorServiceException if tuples cannot be retrieved from database
   */
  @Transactional
  @Override
  public <T extends Tuple<T, F>, F extends Field> byte[] getTupleList(
      Class<T> tupleCls, F field, long count, UUID requestId) {
    // Was passiert, wenn Castor sehr Langsam ist? Hat das Auswirkungen auf die
    // Ephemeral-Execution-Zeit?
    //    try {
    //      TimeUnit.MILLISECONDS.sleep(10);
    //      System.out.println("Slept for 500 millisecs");
    //    }catch(Exception ignored){System.out.println("Didn't sleep");}
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    String reservationId = requestId + "_" + tupleType;
    Reservation reservation;
    if (castorServiceProperties.isMaster()) {
      reservation =
          reservationCachingService.lockAndRetrieveReservation(reservationId, tupleType, count);
      if (reservation == null) {
        reservation = reservationCachingService.createReservation(reservationId, tupleType, count);
        log.debug("Reservation successfully activated on all slaves.");
      }
    } else {
      reservation =
          reservationCachingService.getReservationWithRetry(reservationId, tupleType, count);
    }
    byte[] result = consumeReservation(tupleCls, field, reservation);
    deleteReservedFragments(reservation);
    reservationCachingService.forgetReservation(reservationId);
    return result;
  }

  /**
   * @throws CastorServiceException if tuples cannot be retrieved from database
   */
  private <T extends Tuple<T, F>, F extends Field> byte[] consumeReservation(
      Class<T> tupleCls, F field, Reservation reservation) {

    try (ByteArrayOutputStream tupleBytes = new ByteArrayOutputStream()) {

      for (ReservationElement reservationElement : reservation.getReservations()) {
        try (InputStream tplData = downloadTuples(tupleCls, field, reservationElement)) {
          tupleBytes.write(tplData);
        }
      }

      return tupleBytes.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @throws CastorServiceException if tuples cannot be retrieved from database
   */
  private <T extends Tuple<T, F>, F extends Field> InputStream downloadTuples(
      Class<T> tupleCls, F field, ReservationElement reservationElement) {
    UUID tupleChunkId = reservationElement.getTupleChunkId();
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    final long offset = reservationElement.getStartIndex() * tupleType.getTupleSize();
    final long length = reservationElement.getReservedTuples() * tupleType.getTupleSize();
    try {
      return tupleStore.downloadTuplesAsBytes(tupleCls, field, tupleChunkId, offset, length);
    } catch (Exception e) {
      throw new CastorServiceException(FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG, e);
    }
  }

  /**
   * @throws CastorServiceException if metadata cannot be updated
   */
  private void deleteReservedFragments(Reservation reservation) {
    // fragmentStorageService.deleteAllForReservationId(reservation.getReservationId());
    for (ReservationElement reservationElement : reservation.getReservations()) {
      if (!fragmentStorageService.isChunkReferencedByFragments(
          reservationElement.getTupleChunkId())) {
        tupleStore.deleteTupleChunk(reservationElement.getTupleChunkId());
      }
    }
  }
}
