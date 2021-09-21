/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class ReservationCachingService {
  public static final String NO_RESERVATION_FOR_ID_EXCEPTION_MSG =
      "No reservation was found for requestId %s.";
  public static final String RESERVATION_CONFLICT_EXCEPTION_MSG =
      "Reservation conflict. Reservation with ID #%s already exists.";
  public static final String NO_METADATA_FOR_REFERENCED_CHUNK_EXCEPTION_MSG =
      "No metadata for referenced Chunk #%s available.";
  public static final String REFERENCED_CHUNK_NOT_ACTIVATED_EXCEPTION_MSG =
      "The referenced Chunk #%s has not yet been activated.";
  public static final String TUPLE_TYPE_MISMATCH_EXCEPTION_MSG =
      "The referenced Chunk does not hold the expected tuple type. Actual: %s; requested: %s";
  public static final String TUPLES_NOT_AVAILABLE_EXCEPTION_MSG =
      "Referenced tuples have already been consumed or reserved.";
  public static final String NOT_ENOUGH_TUPLES_IN_CHUNK_EXCEPTION_MSG =
      "Referenced Chunk does not have the required amount (%s) of tuples available (%s).";
  public static final String FAILED_UPDATING_RESERVATION_EXCEPTION_MSG =
      "Failed updating reservation marker for chunk #%s.";
  private final ConsumptionCachingService consumptionCachingService;
  private final RedisTemplate<String, Object> redisTemplate;
  private final TupleChunkMetaDataStorageService tupleChunkMetaDataStorageService;
  private final String cachePrefix;

  @Autowired
  public ReservationCachingService(
      CastorCacheProperties castorCacheProperties,
      ConsumptionCachingService consumptionCachingService,
      RedisTemplate<String, Object> redisTemplate,
      TupleChunkMetaDataStorageService tupleChunkMetaDataStorageService) {
    this.consumptionCachingService = consumptionCachingService;
    this.redisTemplate = redisTemplate;
    this.tupleChunkMetaDataStorageService = tupleChunkMetaDataStorageService;
    this.cachePrefix = CacheKeyPrefix.simple().compute(castorCacheProperties.getReservationStore());
  }

  /**
   * Stores the given {@link Reservation} in cache if no {@link Reservation} with the same ID is
   * present.
   *
   * <p>Reserving tuples will invoke tuple consumption (see {@link ConsumptionCachingService}) since
   * the related tuples are no longer available for other purpose.
   *
   * @throws CastorServiceException if no information about the referenced {@link TupleChunk} is
   *     available
   * @throws CastorServiceException if a referenced {@link TupleChunk} has not yet been activated
   * @throws CastorServiceException if the referenced {@link Tuple}s in the given {@link TupleChunk}
   *     are already consumed
   * @throws CastorServiceException if the cache already holds a reservation with the given ID
   */
  @Transactional
  public void keepReservation(Reservation reservation) {
    validateReservation(reservation);
    log.debug("persisting reservation {}", reservation);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    if (ops.get(cachePrefix + reservation.getReservationId()) == null) {
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("put in database at {}", cachePrefix + reservation.getReservationId());
      for (ReservationElement element : reservation.getReservations()) {
        try {
          tupleChunkMetaDataStorageService.updateReservationForTupleChunkData(
              element.getTupleChunkId(), element.getReservedTuples());
        } catch (CastorClientException cce) {
          throw new CastorServiceException(
              String.format(FAILED_UPDATING_RESERVATION_EXCEPTION_MSG, element.getTupleChunkId()),
              cce);
        }
      }
      log.debug("markers updated.");
      consumptionCachingService.keepConsumption(
          System.currentTimeMillis(),
          reservation.getTupleType(),
          reservation.getReservations().stream()
              .mapToLong(ReservationElement::getReservedTuples)
              .sum());
      log.debug("consumption emitted");
    } else {
      throw new CastorServiceException(
          String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservation.getReservationId()));
    }
  }

  /**
   * @throws CastorServiceException if a reference to a specified {@link TupleChunk} cannot be
   *     resolved
   * @throws CastorServiceException if a referenced {@link TupleChunk} has not yet been activated
   * @throws CastorServiceException if a referenced {@link TupleChunk} does not contain the
   *     requested {@link TupleType}
   * @throws CastorServiceException if a referenced {@link Tuple}s in a given {@link TupleChunk} are
   *     already consumed
   * @throws CastorServiceException if a referenced {@link TupleChunk} does not hold the requested
   *     number of {@link Tuple}s
   */
  private void validateReservation(Reservation reservation) {
    for (ReservationElement element : reservation.getReservations()) {
      TupleChunkMetaDataEntity metaData =
          tupleChunkMetaDataStorageService.getTupleChunkData(element.getTupleChunkId());
      if (metaData == null) {
        throw new CastorServiceException(
            String.format(
                NO_METADATA_FOR_REFERENCED_CHUNK_EXCEPTION_MSG, element.getTupleChunkId()));
      }
      if (metaData.getStatus() != ActivationStatus.UNLOCKED) {
        throw new CastorServiceException(
            String.format(REFERENCED_CHUNK_NOT_ACTIVATED_EXCEPTION_MSG, element.getTupleChunkId()));
      }
      if (metaData.getTupleType() != reservation.getTupleType()) {
        throw new CastorServiceException(
            String.format(
                TUPLE_TYPE_MISMATCH_EXCEPTION_MSG,
                metaData.getTupleType(),
                reservation.getTupleType()));
      }
      if (metaData.getReservedMarker() != element.getStartIndex()) {
        throw new CastorServiceException(TUPLES_NOT_AVAILABLE_EXCEPTION_MSG);
      }
      if ((metaData.getNumberOfTuples() - metaData.getReservedMarker())
          < element.getReservedTuples()) {
        throw new CastorServiceException(
            String.format(
                NOT_ENOUGH_TUPLES_IN_CHUNK_EXCEPTION_MSG,
                element.getReservedTuples(),
                metaData.getNumberOfTuples() - metaData.getReservedMarker()));
      }
    }
  }

  /**
   * Updates the status of a {@link Reservation} with the given id cache.
   *
   * @param reservationId Id of the {@link Reservation} to update.
   * @param status the new {@link ActivationStatus} to be applied on the stored reservation
   * @throws CastorServiceException if no {@link Reservation} is associated with the given
   *     reservation's ID
   */
  @Transactional
  public void updateReservation(String reservationId, ActivationStatus status) {
    log.debug("updating reservation {}", reservationId);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    Object value = ops.get(cachePrefix + reservationId);
    log.debug("object in cache at {} is {}", cachePrefix + reservationId, value);
    Reservation reservation = (value != null) ? (Reservation) value : null;
    if (reservation != null) {
      reservation.setStatus(status);
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("reservation updated");
    } else {
      throw new CastorServiceException(
          String.format(NO_RESERVATION_FOR_ID_EXCEPTION_MSG, reservationId));
    }
  }

  /**
   * @return the {@link Reservation} with the given ID from cache, or null if no {@link Reservation}
   *     is associated with the specified ID
   */
  @Nullable
  @Transactional(readOnly = true)
  public Reservation getReservation(String reservationId) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    return (Reservation) ops.get(cachePrefix + reservationId);
  }

  @Transactional
  public void forgetReservation(String reservationId) {
    redisTemplate.delete(cachePrefix + reservationId);
  }
}
