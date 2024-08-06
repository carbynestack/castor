/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.download.DedicatedTransactionService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  public static final String RESERVATION_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT =
      "No fragment found to fulfill given reservation: %s.";
  public static final String NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG =
      "No released tuple reservation was found for the given request ID #%s.";
  public static final String NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG =
      "Operating as master even though service is not declared to be the master.";
  public static final String RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG =
      "Reservation does not match expected specification (id: \"%s\", tupleType: \"%s\", count:"
          + " %d): %s.";
  public static final String FAILED_CREATE_RESERVATION_EXCEPTION_MSG =
      "Creating the reservation failed.";

  public static final String FAILED_LOCKING_RESERVATION_EXCEPTION_FORMAT =
      "Acquiring an exclusive lock on reservation %s failed.";

  private final ConsumptionCachingService consumptionCachingService;
  private final RedisTemplate<String, Object> redisTemplate;
  private final TupleChunkFragmentStorageService tupleChunkFragmentStorageService;
  private final String cachePrefix;
  private final CastorSlaveServiceProperties slaveServiceProperties;
  private final Optional<CastorInterVcpClient> castorInterVcpClientOptional;
  private final Optional<DedicatedTransactionService> dedicatedTransactionServiceOptional;
  private final CastorServiceProperties castorServiceProperties;

  protected ExecutorService executorService = Executors.newCachedThreadPool();

  @Autowired
  public ReservationCachingService(
      CastorCacheProperties castorCacheProperties,
      ConsumptionCachingService consumptionCachingService,
      RedisTemplate<String, Object> redisTemplate,
      TupleChunkFragmentStorageService tupleChunkFragmentStorageService,
      CastorSlaveServiceProperties slaveServiceProperties,
      CastorServiceProperties castorServiceProperties,
      Optional<CastorInterVcpClient> castorInterVcpClientOptional,
      Optional<DedicatedTransactionService> dedicatedTransactionServiceOptional) {
    this.consumptionCachingService = consumptionCachingService;
    this.redisTemplate = redisTemplate;
    this.tupleChunkFragmentStorageService = tupleChunkFragmentStorageService;
    this.cachePrefix = CacheKeyPrefix.simple().compute(castorCacheProperties.getReservationStore());
    this.slaveServiceProperties = slaveServiceProperties;
    this.castorInterVcpClientOptional = castorInterVcpClientOptional;
    this.dedicatedTransactionServiceOptional = dedicatedTransactionServiceOptional;
    this.castorServiceProperties = castorServiceProperties;
  }

  /**
   * Stores the given {@link Reservation} in cache if no {@link Reservation} with the same ID is
   * present.
   *
   * <p>Reserving tuples will invoke tuple consumption (see {@link ConsumptionCachingService}) since
   * the related tuples are no longer available for other purpose.
   *
   * @throws CastorServiceException if the tuples could not be reserved as requested.
   * @throws CastorServiceException if the cache already holds a reservation with the given ID
   */
  @Transactional
  public void keepReservation(Reservation reservation) {
    log.debug("persisting reservation {}", reservation);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    if (ops.get(cachePrefix + reservation.getReservationId()) == null) {
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("put in database at {}", cachePrefix + reservation.getReservationId());
      applyConsumption(reservation);
      log.debug("consumption emitted");
    } else {
      throw new CastorServiceException(
          String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservation.getReservationId()));
    }
  }

  /**
   * Stores the given {@link Reservation} in cache if no {@link Reservation} with the same ID is
   * present and updates the {@link TupleChunkFragmentEntity tuple fragments} as described by the
   * given {@link Reservation}.
   *
   * <p>This method will invoke tuple consumption (see {@link ConsumptionCachingService}) since the
   * related tuples are no longer available for other purpose.
   *
   * @param reservation The {@link Reservation} to process.
   * @throws CastorServiceException if the tuples could not be reserved as requested.
   * @throws CastorServiceException if the cache already holds a reservation with the given ID
   */
  public void keepAndApplyReservation(Reservation reservation) {
    log.debug("persisting reservation {}", reservation);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    int firstRoundFragment = 0;
    if (ops.get(cachePrefix + reservation.getReservationId()) == null) {
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("put in database at {}", cachePrefix + reservation.getReservationId());
      log.debug("Apply reservation {}", reservation);
      for (ReservationElement re : reservation.getReservations()) {
        if (re.getReservedTuples() == castorServiceProperties.getInitialFragmentSize()) break;
        firstRoundFragment++;
        log.debug("Processing fragmented reservation element {}", re);
        long startIndex = re.getStartIndex();
        long endIndex = startIndex + re.getReservedTuples();
        while (startIndex < endIndex) {
          TupleChunkFragmentEntity fragment =
              tupleChunkFragmentStorageService
                  .findAvailableFragmentForChunkContainingIndex(re.getTupleChunkId(), startIndex)
                  .orElseThrow(
                      () ->
                          new CastorServiceException(
                              String.format(
                                  RESERVATION_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, reservation)));
          if (fragment.getStartIndex() < startIndex) {
            fragment = tupleChunkFragmentStorageService.splitBefore(fragment, startIndex);
          }
          if (endIndex < fragment.getEndIndex()) {
            fragment = tupleChunkFragmentStorageService.splitAt(fragment, endIndex);
          }
          fragment.setReservationId(reservation.getReservationId());
          tupleChunkFragmentStorageService.update(fragment);
          startIndex = fragment.getEndIndex();
        }
      }
      if (firstRoundFragment < reservation.getReservations().size()) {
        HashMap<UUID, ArrayList<Long>> mappedReservations = new HashMap<>();
        List<ReservationElement> roundReservationElements =
            reservation
                .getReservations()
                .subList(firstRoundFragment, reservation.getReservations().size());
        roundReservationElements.forEach(
            resElem -> {
              mappedReservations.computeIfAbsent(resElem.getTupleChunkId(), k -> new ArrayList<>());
              mappedReservations.get(resElem.getTupleChunkId()).add(resElem.getStartIndex());
            });
        int actuallyReserved = 0;
        for (UUID tChunkId : mappedReservations.keySet()) {
          actuallyReserved +=
              tupleChunkFragmentStorageService.reserveRoundFragmentsByIndices(
                  mappedReservations.get(tChunkId), reservation.getReservationId(), tChunkId);
        }
        if (actuallyReserved != roundReservationElements.size())
          throw new CastorServiceException(
              String.format(RESERVATION_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, reservation));
      }
      applyConsumption(reservation);
      log.debug("consumption emitted");
    } else {
      throw new CastorServiceException(
          String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservation.getReservationId()));
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
   * Composes and persists a {@link Reservation} according to the given parameters. When successful,
   * the reservation will be shared with all declared followers.
   *
   * <p>This behaviour is supposed to be performed by declared Castor master services (see {@link
   * CastorServiceProperties#isMaster()}) only. The method will throw an exception if called by a
   * Castor follower service.
   *
   * @param reservationId Identifier for the new {@link Reservation}.
   * @param tupleType Type of tuples to be reserved.
   * @param count Number of tuples to reserve.
   * @throws CastorServiceException if reservation was not created successfully
   * @throws CastorServiceException if reservation was not activated successfully
   * @throws CastorServiceException if reservation was not shared successfully
   * @throws CastorServiceException if no reservation could be made for the given configuration
   * @throws CastorServiceException if method is called even tho this castor service is no master
   */
  @Transactional
  public Reservation createReservation(String reservationId, TupleType tupleType, long count) {
    if (!dedicatedTransactionServiceOptional.isPresent()
        || !castorInterVcpClientOptional.isPresent()) {
      throw new CastorServiceException(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG);
    }
    Reservation reservation;
    try {
      reservation =
          dedicatedTransactionServiceOptional
              .get()
              .runAsNewTransaction(
                  new CreateReservationSupplier(
                      castorInterVcpClientOptional.get(),
                      this,
                      tupleChunkFragmentStorageService,
                      reservationId,
                      tupleType,
                      castorServiceProperties,
                      count));

      if (tupleChunkFragmentStorageService.lockReservedFragmentsWithoutRetrieving(reservationId)
          != reservation.getReservations().size()) {
        throw new CastorServiceException(
            String.format(
                FAILED_LOCKING_RESERVATION_EXCEPTION_FORMAT, reservation.getReservationId()));
      }
    } catch (CastorClientException cce) {
      throw new CastorServiceException(FAILED_CREATE_RESERVATION_EXCEPTION_MSG, cce);
    }
    return reservation;
  }

  /**
   * Retrieves a reservation matching the given criteria from cache. The request will be retried
   * periodically for a defined timeout ({@link
   * CastorSlaveServiceProperties#getWaitForReservationTimeout()} if no reservation is available.
   *
   * <p>The reservation is queried in a detached transaction, as the expected reservation is
   * received and stored in an independent process in the background.
   *
   * @throws CastorServiceException if no unlocked {@link Reservation} with the given id could be
   *     obtained within a defined timout (see {@link
   *     CastorSlaveServiceProperties#getWaitForReservationTimeout()}).
   * @throws CastorServiceException if reservation with given ID is available but does not match
   *     given criteria
   */
  @Transactional(readOnly = true)
  public Reservation getReservationWithRetry(
      String reservationId, TupleType tupleType, long numberOfTuples) {
    Reservation reservation = null;
    WaitForReservationCallable waitForReservationCallable =
        new WaitForReservationCallable(
            reservationId, tupleType, numberOfTuples, this, slaveServiceProperties.getRetryDelay());
    try {
      reservation =
          executorService
              .submit(waitForReservationCallable)
              .get(slaveServiceProperties.getWaitForReservationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      waitForReservationCallable.cancel();
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("getReservationWithRetry threw exception.", e);
      waitForReservationCallable.cancel();
    }
    if (reservation == null) {
      throw new CastorServiceException(
          String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId));
    }
    return reservation;
  }

  /**
   * @return the {@link Reservation} with the given ID from cache, or {@code null} null if either no
   *     {@link Reservation} is associated with the specified ID or the reservation is {@link
   *     ActivationStatus#LOCKED}
   * @throws CastorServiceException if reservation with given ID is available but does not match
   *     given criteria
   */
  @Nullable
  @Transactional(readOnly = true)
  public Reservation getUnlockedReservation(String reservationId, TupleType tupleType, long count) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    Reservation reservation = (Reservation) ops.get(cachePrefix + reservationId);
    if (reservation != null) {
      if (reservation.getStatus() == ActivationStatus.LOCKED) {
        reservation = null;
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

  @Transactional
  public Reservation lockAndRetrieveReservation(
      String reservationId, TupleType tupleType, long count) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    Reservation reservation = (Reservation) ops.get(cachePrefix + reservationId);
    if (reservation != null) {
      long expectedLockedFragments =
          count % castorServiceProperties.getInitialFragmentSize() == 0
              ? count / castorServiceProperties.getInitialFragmentSize()
              : count / castorServiceProperties.getInitialFragmentSize() + 1;
      int lockedFragments =
          tupleChunkFragmentStorageService.lockReservedFragmentsWithoutRetrieving(reservationId)
              * 1000;
      if (lockedFragments == 0) return null;
      if (reservation.getTupleType() != tupleType
          || reservation.getReservations().stream()
                  .mapToLong(ReservationElement::getReservedTuples)
                  .sum()
              != count
          || lockedFragments < expectedLockedFragments) {
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

  @Transactional
  public void forgetReservation(String reservationId) {
    redisTemplate.delete(cachePrefix + reservationId);
  }

  protected void applyConsumption(Reservation reservation) {
    consumptionCachingService.keepConsumption(
        System.currentTimeMillis(),
        reservation.getTupleType(),
        reservation.getReservations().stream()
            .mapToLong(ReservationElement::getReservedTuples)
            .sum());
  }
}
