/*
 * Copyright (c) 2024 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.service.persistence.cache.CreateReservationSupplier.*;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CreateReservationSupplierTest {
  @Mock private CastorInterVcpClient castorInterVcpClientMock;
  @Mock private ReservationCachingService reservationCachingServiceMock;
  @Mock private TupleChunkFragmentStorageService tupleChunkFragmentStorageServiceMock;

  @Mock private CastorServiceProperties castorServicePropertiesMock;

  private final TupleType tupleType = INPUT_MASK_GFP;
  private final String reservationId = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad" + tupleType;
  private final long count = 42;

  private CreateReservationSupplier createReservationSupplier;

  @BeforeEach
  public void setUp() {
    lenient().doReturn(1000).when(castorServicePropertiesMock).getInitialFragmentSize();
    createReservationSupplier =
        new CreateReservationSupplier(
            tupleChunkFragmentStorageServiceMock,
            reservationId,
            tupleType,
            castorServicePropertiesMock,
            count);
  }

  @Test
  void givenInsufficientTuples_whenGet_thenThrowCastorServiceException() {
    // when(tupleChunkFragmentStorageServiceMock.getAvailableTuples(tupleType)).thenReturn(count -
    // 1);
    doReturn(Optional.empty())
        .when(tupleChunkFragmentStorageServiceMock)
        .retrieveSinglePartialFragment(isA(TupleType.class), isA(boolean.class));
    doReturn(1000).when(castorServicePropertiesMock).getInitialFragmentSize();
    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> createReservationSupplier.get());

    assertEquals(FAILED_FETCH_AVAILABLE_FRAGMENT_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void givenProvidedChunksDoNotHaveEnoughTuplesAvailable_whenGet_thenThrowCastorServiceException() {
    doReturn(Optional.empty())
        .when(tupleChunkFragmentStorageServiceMock)
        .retrieveSinglePartialFragment(tupleType, true);
    doReturn(1000).when(castorServicePropertiesMock).getInitialFragmentSize();
    when(tupleChunkFragmentStorageServiceMock.retrieveSinglePartialFragment(tupleType, true))
        .thenReturn(Optional.empty());

    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> createReservationSupplier.get());

    assertEquals(FAILED_FETCH_AVAILABLE_FRAGMENT_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void givenSuccessfulRequest_whenGet_thenReturnExpectedReservation() {
    UUID chunkId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    long startIndex = 0;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(chunkId, tupleType, startIndex, count + 1);
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, count, startIndex);
    Reservation expectedReservation =
        new Reservation(reservationId, tupleType, singletonList(expectedReservationElement));

    doReturn(Optional.of(fragmentEntity))
        .when(tupleChunkFragmentStorageServiceMock)
        .retrieveSinglePartialFragment(tupleType, true);
    when(tupleChunkFragmentStorageServiceMock.splitAt(fragmentEntity, count))
        .thenReturn(fragmentEntity);

    assertEquals(expectedReservation, createReservationSupplier.get());

    verify(tupleChunkFragmentStorageServiceMock, times(1)).splitAt(fragmentEntity, count);
    verify(tupleChunkFragmentStorageServiceMock, times(1)).update(fragmentEntity);
    // verify(castorInterVcpClientMock, times(1)).shareReservation(expectedReservation);
  }
}
