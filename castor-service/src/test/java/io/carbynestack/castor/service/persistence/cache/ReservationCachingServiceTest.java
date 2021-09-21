/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.service.persistence.cache.ReservationCachingService.*;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@RunWith(MockitoJUnitRunner.class)
public class ReservationCachingServiceTest {
  @Mock private ConsumptionCachingService consumptionCachingServiceMock;

  @Mock private RedisTemplate<String, Object> redisTemplateMock;

  @Mock private ValueOperations<String, Object> valueOperationsMock;

  @Mock private CastorCacheProperties castorCachePropertiesMock;

  @Mock private TupleChunkMetaDataStorageService tupleChunkMetaDataStorageServiceMock;

  private final String testCacheName = "testCache";
  private final String testCachePrefix = CacheKeyPrefix.simple().compute(testCacheName);

  private ReservationCachingService reservationCachingService;

  @Before
  public void setUp() {
    when(castorCachePropertiesMock.getReservationStore()).thenReturn(testCacheName);
    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    this.reservationCachingService =
        new ReservationCachingService(
            castorCachePropertiesMock,
            consumptionCachingServiceMock,
            redisTemplateMock,
            tupleChunkMetaDataStorageServiceMock);
  }

  @Test
  public void givenReferencedChunkIsNotKnown_whenKeepReservation_throwCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    Reservation reservationMock = mock(Reservation.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(null);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(NO_METADATA_FOR_REFERENCED_CHUNK_EXCEPTION_MSG, chunkId),
        actualCse.getMessage());
  }

  @Test
  public void givenReferencedChunkIsNotUnlocked_whenKeepReservation_throwCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.LOCKED);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(REFERENCED_CHUNK_NOT_ACTIVATED_EXCEPTION_MSG, chunkId),
        actualCse.getMessage());
  }

  @Test
  public void
      givenReferencedChunkTupleTypeMismatch_whenKeepReservation_throwCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    TupleType requestedTupleType = TupleType.INPUT_MASK_GFP;
    TupleType actualTupleType = TupleType.SQUARE_TUPLE_GF2N;
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationMock.getTupleType()).thenReturn(requestedTupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(metaDataMock.getTupleType()).thenReturn(actualTupleType);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(TUPLE_TYPE_MISMATCH_EXCEPTION_MSG, actualTupleType, requestedTupleType),
        actualCse.getMessage());
  }

  @Test
  public void
      givenTuplesOfReferencedChunkNotAvailable_whenKeepReservation_throwCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    long reservedMarker = 5;
    long startIndex = 0;
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationElementMock.getStartIndex()).thenReturn(startIndex);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(metaDataMock.getTupleType()).thenReturn(tupleType);
    when(metaDataMock.getReservedMarker()).thenReturn(reservedMarker);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(TUPLES_NOT_AVAILABLE_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  public void givenChunkHasNotEnoughTuples_whenKeepReservation_throwCastorServiceException() {
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    long tupleStartIndex = 5;
    long requestedTuples = 10;
    long totalTuplesInChunk = 10;
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationElementMock.getStartIndex()).thenReturn(tupleStartIndex);
    when(reservationElementMock.getReservedTuples()).thenReturn(requestedTuples);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(metaDataMock.getTupleType()).thenReturn(tupleType);
    when(metaDataMock.getReservedMarker()).thenReturn(tupleStartIndex);
    when(metaDataMock.getNumberOfTuples()).thenReturn(totalTuplesInChunk);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(
            NOT_ENOUGH_TUPLES_IN_CHUNK_EXCEPTION_MSG,
            requestedTuples,
            totalTuplesInChunk - tupleStartIndex),
        actualCse.getMessage());
  }

  @Test
  public void
      givenUpdateReservedTuplesForChunkFails_whenKeepReservation_thenThrowCastorServiceException() {
    CastorClientException expectedException = new CastorClientException("expected");
    long consumption = 42;
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    String reservationId = "reservationId";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationElementMock.getReservedTuples()).thenReturn(consumption);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(metaDataMock.getReservedMarker()).thenReturn(0L);
    when(metaDataMock.getNumberOfTuples()).thenReturn(consumption);
    when(metaDataMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);
    doThrow(expectedException)
        .when(tupleChunkMetaDataStorageServiceMock)
        .updateReservationForTupleChunkData(chunkId, consumption);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(FAILED_UPDATING_RESERVATION_EXCEPTION_MSG, chunkId), actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(consumptionCachingServiceMock, never())
        .keepConsumption(anyLong(), any(TupleType.class), anyLong());
  }

  @Test
  public void
      givenNoReservationWithGivenIdInCache_whenKeepReservation_thenStoreInCacheAndEmitConsumption() {
    long consumption = 42;
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    String reservationId = "reservationId";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkMetaDataEntity metaDataMock = mock(TupleChunkMetaDataEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationElementMock.getReservedTuples()).thenReturn(consumption);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(chunkId)).thenReturn(metaDataMock);
    when(metaDataMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(metaDataMock.getReservedMarker()).thenReturn(0L);
    when(metaDataMock.getNumberOfTuples()).thenReturn(consumption);
    when(metaDataMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);

    reservationCachingService.keepReservation(reservationMock);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(tupleChunkMetaDataStorageServiceMock)
        .updateReservationForTupleChunkData(chunkId, consumption);
    verify(consumptionCachingServiceMock)
        .keepConsumption(anyLong(), eq(tupleType), eq(consumption));
  }

  @Test
  public void
      givenReservationWithSameIdInCache_whenKeepReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);

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
  public void
      givenNoReservationWithIdInCache_whenUpdateReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.updateReservation(reservationId, newStatus));

    assertEquals(
        String.format(NO_RESERVATION_FOR_ID_EXCEPTION_MSG, reservationId), actualCse.getMessage());
  }

  @Test
  public void givenSuccessfulRequest_whenUpdateReservation_thenUpdateEntityInCache() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    reservationCachingService.updateReservation(reservationId, newStatus);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(reservationMock).setStatus(newStatus);
  }

  @Test
  public void givenReservationWithIdInCache_whenGetReservation_thenReturnExpectedReservation() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);

    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    assertEquals(reservationMock, reservationCachingService.getReservation(reservationId));
  }

  @Test
  public void givenSuccessfulRequest_whenForgetReservation_thenCallDeleteOnCache() {
    String reservationId = "reservationId";

    reservationCachingService.forgetReservation(reservationId);

    verify(redisTemplateMock).delete(testCachePrefix + reservationId);
  }
}
