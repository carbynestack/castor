/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.service.download.DefaultTuplesDownloadService.FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.io.ByteArrayInputStream;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTuplesDownloadServiceTest {

  @Mock private TupleStore tupleStoreMock;
  @Mock private TupleChunkFragmentStorageService tupleChunkFragmentStorageServiceMock;
  @Mock private ReservationCachingService reservationCachingServiceMock;
  @Mock private CastorServiceProperties castorServicePropertiesMock;

  private DefaultTuplesDownloadService tuplesDownloadService;

  @Before
  public void setUp() {
    tuplesDownloadService =
        new DefaultTuplesDownloadService(
            tupleStoreMock,
            tupleChunkFragmentStorageServiceMock,
            reservationCachingServiceMock,
            castorServicePropertiesMock);
  }

  @SneakyThrows
  @Test
  public void givenTuplesCannotBeRetrieved_whenGetTuplesAsSlave_thenThrowCastorServiceException() {
    UUID requestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    TupleType tupleType = INPUT_MASK_GFP;
    String resultingReservationId = requestId + "_" + tupleType;
    long count = 42;
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    Reservation reservationMock = mock(Reservation.class);
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    CastorServiceException expectedCause = new CastorServiceException("expected");

    when(castorServicePropertiesMock.isMaster()).thenReturn(false);
    when(reservationElementMock.getReservedTuples()).thenReturn(count);
    when(reservationElementMock.getStartIndex()).thenReturn(0L);
    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);

    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(reservationCachingServiceMock.getReservationWithRetry(
            resultingReservationId, tupleType, count))
        .thenReturn(reservationMock);
    when(tupleStoreMock.downloadTuples(
            tupleType.getTupleCls(),
            tupleType.getField(),
            chunkId,
            0,
            count * tupleType.getTupleSize()))
        .thenThrow(expectedCause);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    assertEquals(FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedCause, actualCse.getCause());
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenGetTuplesAsMaster_thenReturnExpectedTuples() {
    TupleType tupleType = INPUT_MASK_GFP;
    long count = 2;
    UUID requestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    String resultingReservationId = requestId + "_" + tupleType;
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    long reStartIndex = 0;
    ReservationElement availableReservationElement =
        new ReservationElement(chunkId, count, reStartIndex);
    Reservation availableReservation =
        new Reservation(
                resultingReservationId, tupleType, singletonList(availableReservationElement))
            .setStatus(ActivationStatus.UNLOCKED);
    long expectedTupleDownloadLength = tupleType.getTupleSize() * count;
    byte[] tupleData = RandomUtils.nextBytes((int) expectedTupleDownloadLength);
    TupleList expectedTupleList =
        TupleList.fromStream(
            tupleType.getTupleCls(),
            tupleType.getField(),
            new ByteArrayInputStream(tupleData),
            tupleData.length);

    when(castorServicePropertiesMock.isMaster()).thenReturn(true);
    when(reservationCachingServiceMock.createReservation(resultingReservationId, tupleType, count))
        .thenReturn(availableReservation);
    when(tupleStoreMock.downloadTuples(
            tupleType.getTupleCls(), tupleType.getField(), chunkId, 0, expectedTupleDownloadLength))
        .thenReturn(expectedTupleList);

    assertEquals(
        expectedTupleList,
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    verify(tupleStoreMock).deleteTupleChunk(chunkId);
    verify(reservationCachingServiceMock).forgetReservation(resultingReservationId);
  }
}
