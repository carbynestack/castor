/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.service.download.CreateReservationSupplier.*;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CreateReservationSupplierTest {
  @Mock private CastorInterVcpClient castorInterVcpClientMock;
  @Mock private ReservationCachingService reservationCachingServiceMock;
  @Mock private TupleChunkMetaDataStorageService tupleChunkMetaDataStorageServiceMock;

  private final TupleType tupleType = INPUT_MASK_GFP;
  private final String reservationId = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad" + tupleType;
  private final long count = 42;

  private CreateReservationSupplier createReservationSupplier;

  @Before
  public void setUp() {
    createReservationSupplier =
        new CreateReservationSupplier(
            castorInterVcpClientMock,
            reservationCachingServiceMock,
            tupleChunkMetaDataStorageServiceMock,
            reservationId,
            tupleType,
            count);
  }

  @Test
  public void givenInsufficientTuples_whenGet_thenThrowCastorServiceException() {
    when(tupleChunkMetaDataStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count - 1);

    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> createReservationSupplier.get());

    assertEquals(
        String.format(INSUFFICIENT_TUPLES_EXCEPTION_MSG, tupleType, count - 1, count),
        actualCse.getMessage());
  }

  @Test
  public void
      givenProvidedChunksDoNotHaveEnoughTuplesAvailable_whenGet_thenThrowCastorServiceException() {
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);
    when(tupleChunkMetaDataStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(tupleType))
        .thenReturn(singletonList(metaDataEntityMock));
    when(metaDataEntityMock.getTupleChunkId())
        .thenReturn(UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8"));
    when(metaDataEntityMock.getReservedMarker()).thenReturn(0L);
    when(metaDataEntityMock.getNumberOfTuples()).thenReturn(1L);

    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> createReservationSupplier.get());

    assertEquals(
        String.format(FAILED_RESERVE_AMOUNT_TUPLES_EXCEPTION_MSG, tupleType, 1, count),
        actualCse.getMessage());
  }

  @Test
  public void givenSharingReservationFails_whenGet_thenThrowCastorServiceException() {
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);
    UUID chunkId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    long reservedMarker = 0;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, count, reservedMarker);
    Reservation expectedReservation =
        new Reservation(reservationId, tupleType, singletonList(expectedReservationElement));

    when(tupleChunkMetaDataStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(tupleType))
        .thenReturn(singletonList(metaDataEntityMock));
    when(metaDataEntityMock.getTupleChunkId()).thenReturn(chunkId);
    when(metaDataEntityMock.getReservedMarker()).thenReturn(reservedMarker);
    when(metaDataEntityMock.getNumberOfTuples()).thenReturn(count);
    when(castorInterVcpClientMock.shareReservation(expectedReservation)).thenReturn(false);

    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> createReservationSupplier.get());

    assertEquals(SHARING_RESERVATION_FAILED_EXCEPTION_MSG, actualCse.getMessage());
    verify(reservationCachingServiceMock).keepReservation(expectedReservation);
  }

  @Test
  public void givenMultipleChunksWithAvailable_whenGet_thenReturnExpectedReservation() {
    TupleChunkMetaDataEntity metaDataEntity1Mock = mock(TupleChunkMetaDataEntity.class);
    TupleChunkMetaDataEntity metaDataEntity2Mock = mock(TupleChunkMetaDataEntity.class);
    UUID chunk1Id = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    UUID chunk2Id = UUID.fromString("4d206204-96a0-4ddc-b409-4ac05b1f12f2");
    long reservedMarker = 0;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunk1Id, count, reservedMarker);
    Reservation expectedReservation =
        new Reservation(reservationId, tupleType, singletonList(expectedReservationElement));

    when(tupleChunkMetaDataStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(tupleType))
        .thenReturn(Arrays.asList(metaDataEntity1Mock, metaDataEntity2Mock));
    when(metaDataEntity1Mock.getTupleChunkId()).thenReturn(chunk1Id);
    when(metaDataEntity1Mock.getReservedMarker()).thenReturn(reservedMarker);
    when(metaDataEntity1Mock.getNumberOfTuples()).thenReturn(count);
    when(castorInterVcpClientMock.shareReservation(expectedReservation)).thenReturn(true);

    assertEquals(expectedReservation, createReservationSupplier.get());
    verify(reservationCachingServiceMock).keepReservation(expectedReservation);
  }

  @Test
  public void givenFirstChunkHasNotEnoughTuples_whenGet_thenCreateReservationReferencingMultipleChunks() {TupleChunkMetaDataEntity metaDataEntity1Mock = mock(TupleChunkMetaDataEntity.class);
    TupleChunkMetaDataEntity metaDataEntity2Mock = mock(TupleChunkMetaDataEntity.class);
    UUID chunk1Id = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    UUID chunk2Id = UUID.fromString("4d206204-96a0-4ddc-b409-4ac05b1f12f2");
    long reservedMarker = 0;
    List<ReservationElement> expectedReservationElements = Arrays.asList(
            new ReservationElement(chunk1Id, count-1, reservedMarker),
            new ReservationElement(chunk2Id, 1, reservedMarker));
    Reservation expectedReservation =
            new Reservation(reservationId, tupleType, expectedReservationElements);

    when(tupleChunkMetaDataStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count);
    when(tupleChunkMetaDataStorageServiceMock.getTupleChunkData(tupleType))
            .thenReturn(Arrays.asList(metaDataEntity1Mock, metaDataEntity2Mock));
    when(metaDataEntity1Mock.getTupleChunkId()).thenReturn(chunk1Id);
    when(metaDataEntity1Mock.getReservedMarker()).thenReturn(reservedMarker);
    when(metaDataEntity1Mock.getNumberOfTuples()).thenReturn(count-1);
    when(metaDataEntity2Mock.getTupleChunkId()).thenReturn(chunk2Id);
    when(metaDataEntity2Mock.getReservedMarker()).thenReturn(reservedMarker);
    when(metaDataEntity2Mock.getNumberOfTuples()).thenReturn(count);
    when(castorInterVcpClientMock.shareReservation(expectedReservation)).thenReturn(true);

    assertEquals(expectedReservation, createReservationSupplier.get());
    verify(reservationCachingServiceMock).keepReservation(expectedReservation);
  }
}
