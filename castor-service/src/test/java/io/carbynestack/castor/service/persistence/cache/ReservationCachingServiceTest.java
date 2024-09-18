/*
 * Copyright (c) 2024 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static io.carbynestack.castor.service.persistence.cache.CreateReservationSupplier.SHARING_RESERVATION_FAILED_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.cache.ReservationCachingService.*;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class ReservationCachingServiceTest {
  @Mock private ConsumptionCachingService consumptionCachingServiceMock;

  @Mock private RedisTemplate<String, Object> redisTemplateMock;

  @Mock private ValueOperations<String, Object> valueOperationsMock;

  @Mock private CastorCacheProperties castorCachePropertiesMock;

  @Mock private TupleChunkFragmentStorageService tupleChunkFragmentStorageServiceMock;

  @Mock private CastorSlaveServiceProperties castorSlaveServicePropertiesMock;

  @Mock private Optional<CastorInterVcpClient> castorInterVcpClientOptionalMock;

  @Mock private Optional<DedicatedTransactionService> dedicatedTransactionServiceOptionalMock;

  @Mock private ExecutorService executorServiceMock;

  @Mock private CastorServiceProperties castorServicePropertiesMock;

  @Spy private CastorInterVcpClient castorInterVcpClientSpy;

  private final String testCacheName = "testCache";
  private final String testCachePrefix = CacheKeyPrefix.simple().compute(testCacheName);

  private ReservationCachingService reservationCachingService;

  @BeforeEach
  public void setUp() {
    when(castorCachePropertiesMock.getReservationStore()).thenReturn(testCacheName);
    Mockito.lenient().doReturn(1000).when(castorServicePropertiesMock).getInitialFragmentSize();
    if (reservationCachingService == null) {
      this.reservationCachingService =
          new ReservationCachingService(
              castorCachePropertiesMock,
              consumptionCachingServiceMock,
              redisTemplateMock,
              tupleChunkFragmentStorageServiceMock,
              castorSlaveServicePropertiesMock,
              castorServicePropertiesMock,
              castorInterVcpClientOptionalMock,
              dedicatedTransactionServiceOptionalMock);
      this.reservationCachingService.executorService = executorServiceMock;
    }
  }

  @Test
  void
      givenNoReservationWithGivenIdInCache_whenKeepReservation_thenStoreInCacheAndEmitConsumption() {
    long consumption = 42;
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    String reservationId = "reservationId";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    Reservation reservationMock = mock(Reservation.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(reservationElementMock.getReservedTuples()).thenReturn(consumption);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);

    reservationCachingService.keepReservation(reservationMock);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(consumptionCachingServiceMock)
        .keepConsumption(anyLong(), eq(tupleType), eq(consumption));
  }

  @Test
  void givenReservationWithSameIdInCache_whenKeepReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservationId), actualCse.getMessage());
    verify(valueOperationsMock, never()).set(anyString(), any(Reservation.class));
    verify(consumptionCachingServiceMock, never())
        .keepConsumption(anyLong(), any(TupleType.class), anyLong());
  }

  @Test
  void givenNoReservationWithIdInCache_whenUpdateReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.updateReservation(reservationId, newStatus));

    assertEquals(
        String.format(NO_RESERVATION_FOR_ID_EXCEPTION_MSG, reservationId), actualCse.getMessage());
  }

  @Test
  void givenSuccessfulRequest_whenUpdateReservation_thenUpdateEntityInCache() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    reservationCachingService.updateReservation(reservationId, newStatus);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(reservationMock).setStatus(newStatus);
  }

  @Test
  void
      givenReservationWithIdInCache_whenLockAndRetrieveReservation_thenReturnExpectedReservation() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long tupleCount = 42;
    String reservationId = "reservationId";
    ReservationElement existingReservationElement = new ReservationElement(chunkId, tupleCount, 0);
    Reservation existingReservation =
        new Reservation(reservationId, tupleType, singletonList(existingReservationElement))
            .setStatus(ActivationStatus.UNLOCKED);

    doReturn(1)
        .when(tupleChunkFragmentStorageServiceMock)
        .lockReservedFragmentsWithoutRetrieving(isA(UUID.class), anyLong(), isA(String.class));
    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(existingReservation);

    assertEquals(
        existingReservation,
        reservationCachingService.lockAndRetrieveReservation(reservationId, tupleType, tupleCount));
  }

  @Test
  void givenSuccessfulRequest_whenForgetReservation_thenCallDeleteOnCache() {
    String reservationId = "reservationId";

    reservationCachingService.forgetReservation(reservationId);

    verify(redisTemplateMock).delete(testCachePrefix + reservationId);
  }

  @Test
  void givenAlreadyAReservationWithGivenIdInCache_whenKeepAndApplyReservation_thenThrow() {
    String reservationId = "testReservation";
    Reservation existingReservationMock = mock(Reservation.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(existingReservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId))
        .thenReturn(existingReservationMock);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepAndApplyReservation(existingReservationMock));

    assertEquals(
        String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservationId), actualCse.getMessage());
  }

  @Test
  void givenNoFragmentWithReferencedStartIndex_whenKeepAndApplyReservation_thenThrow() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement re =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    Reservation r = new Reservation(reservationId, tupleType, singletonList(re));

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(tupleChunkFragmentStorageServiceMock.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.empty());

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepAndApplyReservation(r));

    assertEquals(
        String.format(RESERVATION_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, r), actualCse.getMessage());
  }

  @Test
  @SneakyThrows
  void
      givenReferencedSequenceLiesWithin_whenKeepAndApplyReservation_thenSplitFragmentAccordingly() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement re =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    Reservation r = new Reservation(reservationId, tupleType, singletonList(re));
    long existingFragmentStartIndex = 0;
    long existingFragmentEndIndex = 99;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            existingFragmentStartIndex,
            existingFragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null,
            true);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(tupleChunkFragmentStorageServiceMock.splitAt(
            existingFragment, requestedStartIndex + requestedLength))
        .thenReturn(existingFragment);
    when(tupleChunkFragmentStorageServiceMock.splitBefore(existingFragment, requestedStartIndex))
        .thenReturn(existingFragment);
    when(tupleChunkFragmentStorageServiceMock.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.of(existingFragment));

    reservationCachingService.keepAndApplyReservation(r);
    verify(tupleChunkFragmentStorageServiceMock, times(1)).update(existingFragment);
    verify(tupleChunkFragmentStorageServiceMock, times(1))
        .splitBefore(existingFragment, requestedStartIndex);
    verify(tupleChunkFragmentStorageServiceMock, times(1))
        .splitAt(existingFragment, requestedStartIndex + requestedLength);
    verify(consumptionCachingServiceMock)
        .keepConsumption(anyLong(), eq(tupleType), eq(requestedLength));
  }

  @Test
  void
      givenNoReservationReceivedInTime_whenGetReservationWithRetry_thenThrowCastorServiceException()
          throws ExecutionException, InterruptedException, TimeoutException {
    String reservationId = "testReservationId";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long tupleCount = 42;
    long retryDelay = 5;
    long reservationTimeout = 0;
    when(castorSlaveServicePropertiesMock.getRetryDelay()).thenReturn(retryDelay);
    when(castorSlaveServicePropertiesMock.getWaitForReservationTimeout())
        .thenReturn(reservationTimeout);
    Future<Reservation> reservationFutureMock = mock(Future.class);

    when(executorServiceMock.submit(any(WaitForReservationCallable.class)))
        .thenReturn(reservationFutureMock);
    when(reservationFutureMock.get(reservationTimeout, TimeUnit.MILLISECONDS))
        .thenThrow(new InterruptedException("expected"));

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.getReservationWithRetry(
                    reservationId, tupleType, tupleCount));
    assertEquals(
        String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId),
        actualCse.getMessage());
  }

  @Test
  void givenSuccessfulRequest_whenGetReservationWithRetry_thenReturnExpectedReservation()
      throws ExecutionException, InterruptedException, TimeoutException {
    String reservationId = "testReservationId";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long tupleCount = 42;
    long retryDelay = 5;
    long reservationTimeout = 10;
    Reservation expectedReservationMock = mock(Reservation.class);
    Future<Reservation> reservationFutureMock = mock(Future.class);

    when(castorSlaveServicePropertiesMock.getRetryDelay()).thenReturn(retryDelay);
    when(castorSlaveServicePropertiesMock.getWaitForReservationTimeout())
        .thenReturn(reservationTimeout);
    when(executorServiceMock.submit(any(WaitForReservationCallable.class)))
        .thenReturn(reservationFutureMock);
    when(reservationFutureMock.get(reservationTimeout, TimeUnit.MILLISECONDS))
        .thenReturn(expectedReservationMock);

    assertEquals(
        expectedReservationMock,
        reservationCachingService.getReservationWithRetry(reservationId, tupleType, tupleCount));
  }

  @Test
  void
      givenDedicatedTransactionServiceNotDefined_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(false);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.createReservation(
                    "testReservationId", INPUT_MASK_GFP, 42));

    assertEquals(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void
      givenCastorInterVcpClientIsNotDefined_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(castorInterVcpClientOptionalMock.isPresent()).thenReturn(false);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.createReservation(
                    "testReservationId", INPUT_MASK_GFP, 42));

    assertEquals(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void
      givenUnlockedReservationWithMatchingConfigurationInCache_whenGetReservation_thenReturnCachedReservation() {
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    long count = 42;
    Reservation expectedReservationMock = mock(Reservation.class);
    ReservationElement reservationElementMock = mock(ReservationElement.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(valueOperationsMock.get(testCachePrefix + reservationId))
        .thenReturn(expectedReservationMock);
    // when(expectedReservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(expectedReservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationElementMock.getReservedTuples()).thenReturn(count);
    when(reservationElementMock.getTupleChunkId()).thenReturn(UUID.randomUUID());
    when(expectedReservationMock.getReservations())
        .thenReturn(singletonList(reservationElementMock));
    doReturn(1)
        .when(tupleChunkFragmentStorageServiceMock)
        .lockReservedFragmentsWithoutRetrieving(isA(UUID.class), anyLong(), isA(String.class));

    assertEquals(
        expectedReservationMock,
        reservationCachingService.lockAndRetrieveReservation(reservationId, tupleType, count));
  }

  @Test
  void
      givenCachedReservationForIdMismatchType_whenLockAndRetrieveReservation_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    TupleType mismatchedTupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    ReservationElement existingReservationElement = new ReservationElement(chunkId, count, 0);
    Reservation existingReservation =
        new Reservation(
                reservationId, mismatchedTupleType, singletonList(existingReservationElement))
            .setStatus(ActivationStatus.UNLOCKED);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(existingReservation);
    doReturn(1)
        .when(tupleChunkFragmentStorageServiceMock)
        .lockReservedFragmentsWithoutRetrieving(isA(UUID.class), anyLong(), isA(String.class));

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.lockAndRetrieveReservation(
                    reservationId, tupleType, count));

    assertEquals(
        String.format(
            RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG,
            reservationId,
            tupleType,
            count,
            existingReservation),
        actualCse.getMessage());
  }

  @Test
  void
      givenCachedReservationForIdMismatchReservedCount_whenLockAndRetrieveReservation_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    long count = 42;
    ReservationElement existingReservationElement = new ReservationElement(chunkId, count - 1, 0);
    Reservation existingReservation =
        new Reservation(reservationId, tupleType, singletonList(existingReservationElement))
            .setStatus(ActivationStatus.UNLOCKED);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(existingReservation);
    doReturn(1)
        .when(tupleChunkFragmentStorageServiceMock)
        .lockReservedFragmentsWithoutRetrieving(isA(UUID.class), anyLong(), isA(String.class));
    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.lockAndRetrieveReservation(
                    reservationId, tupleType, count));

    assertEquals(
        String.format(
            RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG,
            reservationId,
            tupleType,
            count,
            existingReservation),
        actualCse.getMessage());
  }

  @Test
  void
      givenNoCachedReservationButCreationThrows_whenCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    CastorClientException expectedException = new CastorClientException("expected");
    DedicatedTransactionService dedicatedTransactionServiceMock =
        mock(DedicatedTransactionService.class);

    when(castorInterVcpClientOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenThrow(expectedException);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.createReservation(reservationId, INPUT_MASK_GFP, 42));

    assertEquals(FAILED_CREATE_RESERVATION_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
  }

  @Disabled // Disabled as unlocking is at least temporarily not implemented
  @Test
  void
      givenNoCachedReservationActivationThrows_whenCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    CastorClientException expectedException = new CastorClientException("expected");
    DedicatedTransactionService dedicatedTransactionServiceMock =
        mock(DedicatedTransactionService.class);
    Reservation reservationMock = mock(Reservation.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(castorInterVcpClientOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenReturn(reservationMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(UnlockReservationSupplier.class)))
        .thenThrow(expectedException);
    when(castorInterVcpClientOptionalMock.get()).thenReturn(castorInterVcpClientSpy);
    when(castorInterVcpClientSpy.shareReservation(any())).thenReturn(true);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.createReservation(reservationId, INPUT_MASK_GFP, 42));

    assertEquals(FAILED_CREATE_RESERVATION_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
  }

  @Test
  void
      givenNoCachedReservationAndSuccessfulRequest_whenGetOrCreateReservation_thenReturnNewReservation() {
    String reservationId = "testReservationId";
    DedicatedTransactionService dedicatedTransactionServiceMock =
        mock(DedicatedTransactionService.class);
    Reservation expectedReservationMock = mock(Reservation.class);
    ReservationElement firstElementMock = mock(ReservationElement.class);

    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(castorInterVcpClientOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenReturn(expectedReservationMock);
    when(castorInterVcpClientOptionalMock.get()).thenReturn(castorInterVcpClientSpy);
    when(castorInterVcpClientSpy.shareReservation(isA(Reservation.class))).thenReturn(true);
    when(firstElementMock.getStartIndex()).thenReturn(0L);
    when(firstElementMock.getReservedTuples()).thenReturn(42L);
    when(expectedReservationMock.getReservations()).thenReturn(singletonList(firstElementMock));

    doReturn(1)
        .when(tupleChunkFragmentStorageServiceMock)
        .lockReservedFragmentsWithoutRetrieving(any(), any(Long.class), any());

    assertEquals(
        expectedReservationMock,
        reservationCachingService.createReservation(reservationId, INPUT_MASK_GFP, 42));
  }

  @Test
  void givenSharingReservationFails_whenGet_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    long startIndex = 0;
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(chunkId, tupleType, startIndex, 42);
    DedicatedTransactionService dedicatedTransactionServiceSpy =
        spy(DedicatedTransactionService.class);
    Reservation reservationMock = mock(Reservation.class);

    // when(tupleChunkFragmentStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count);
    when(castorInterVcpClientOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(tupleChunkFragmentStorageServiceMock.retrieveSinglePartialFragment(tupleType, true))
        .thenReturn(Optional.of(fragmentEntity));
    // when(castorInterVcpClientSpy.shareReservation(any(Reservation.class))).thenReturn(false);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceSpy);
    // when(dedicatedTransactionServiceMock.runAsNewTransaction(isA(CreateReservationSupplier.class)))
    //        .thenReturn(reservationMock);
    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    when(castorInterVcpClientOptionalMock.get()).thenReturn(castorInterVcpClientSpy);
    when(castorInterVcpClientSpy.shareReservation(isA(Reservation.class))).thenReturn(false);
    when(valueOperationsMock.get(any())).thenReturn(null);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.createReservation("testReservation", tupleType, 42));

    assertEquals(SHARING_RESERVATION_FAILED_EXCEPTION_MSG, actualCse.getMessage());
    verify(tupleChunkFragmentStorageServiceMock, times(1)).update(fragmentEntity);
  }
}
