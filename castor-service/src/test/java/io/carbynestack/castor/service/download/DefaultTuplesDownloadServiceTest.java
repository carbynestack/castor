/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static io.carbynestack.castor.service.download.DefaultTuplesDownloadService.*;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import io.carbynestack.castor.service.persistence.tuplestore.TupleStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
class DefaultTuplesDownloadServiceTest {

  @Mock private TupleStore tupleStoreMock;
  @Mock private TupleChunkMetaDataStorageService tupleChunkMetaDataStorageServiceMock;
  @Mock private ReservationCachingService reservationCachingServiceMock;
  @Mock private CastorSlaveServiceProperties castorSlaveServicePropertiesMock;
  @Mock private Optional<CastorInterVcpClient> castorInterVcpClientOptional;
  @Mock private DedicatedTransactionService dedicatedTransactionServiceMock;
  @Mock private Optional<DedicatedTransactionService> dedicatedTransactionServiceOptionalMock;

  private DefaultTuplesDownloadService tuplesDownloadService;

  @BeforeEach
  public void setUp() {
    tuplesDownloadService =
        new DefaultTuplesDownloadService(
            tupleStoreMock,
            tupleChunkMetaDataStorageServiceMock,
            reservationCachingServiceMock,
            castorSlaveServicePropertiesMock,
            castorInterVcpClientOptional,
            dedicatedTransactionServiceOptionalMock);
  }

  @Test
  void givenNoReservationReceivedInTime_whenObtainReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    long retryDelay = 5;
    long reservationTimeout = 10;
    when(castorSlaveServicePropertiesMock.getRetryDelay()).thenReturn(retryDelay);
    when(castorSlaveServicePropertiesMock.getWaitForReservationTimeout())
        .thenReturn(reservationTimeout);

    try (MockedConstruction<WaitForReservationCallable> awaitReservationMC =
        mockConstruction(
            WaitForReservationCallable.class,
            ((mock, context) -> {
              assertEquals(reservationId, context.arguments().get(0));
              assertEquals(reservationCachingServiceMock, context.arguments().get(1));
              assertEquals(retryDelay, context.arguments().get(2));
              when(mock.call())
                  .thenAnswer(
                      (Answer<Reservation>)
                          invocation -> {
                            Thread.sleep(reservationTimeout * 2);
                            return null;
                          });
            }))) {

      CastorServiceException actualCse =
          assertThrows(
              CastorServiceException.class,
              () -> tuplesDownloadService.obtainReservation(reservationId));
      assertEquals(
          String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId),
          actualCse.getMessage());
      assertEquals(1, awaitReservationMC.constructed().size());
      verify(awaitReservationMC.constructed().get(0)).cancel();
    }
  }

  @Test
  void givenSuccessfulRequest_whenObtainReservation_thenReturnExpectedReservation() {
    String reservationId = "testReservationId";
    long retryDelay = 5;
    long reservationTimeout = 10;
    Reservation expectedReservationMock = mock(Reservation.class);

    when(castorSlaveServicePropertiesMock.getRetryDelay()).thenReturn(retryDelay);
    when(castorSlaveServicePropertiesMock.getWaitForReservationTimeout())
        .thenReturn(reservationTimeout);

    try (MockedConstruction<WaitForReservationCallable> awaitReservationMC =
        mockConstruction(
            WaitForReservationCallable.class,
            (mock, context) -> when(mock.call()).thenReturn(expectedReservationMock))) {

      assertEquals(expectedReservationMock, tuplesDownloadService.obtainReservation(reservationId));
      assertEquals(1, awaitReservationMC.constructed().size());
      verify(awaitReservationMC.constructed().get(0), never()).cancel();
    }
  }

  @Test
  void
      givenDedicatedTransactionServiceNotDefined_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(false);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getOrCreateReservation(
                    "testReservationId", INPUT_MASK_GFP, 42));

    assertEquals(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void
      givenCastorInterVcpClientIsNotDefined_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(castorInterVcpClientOptional.isPresent()).thenReturn(false);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getOrCreateReservation(
                    "testReservationId", INPUT_MASK_GFP, 42));

    assertEquals(NOT_DECLARED_TO_BE_THE_MASTER_EXCEPTION_MSG, actualCse.getMessage());
  }

  @Test
  void
      givenUnlockedReservationWithMatchingConfigurationInCache_whenGetOrCreateReservation_thenReturnCachedReservation() {
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    long count = 42;
    Reservation expectedReservationMock = mock(Reservation.class);
    ReservationElement reservationElementMock = mock(ReservationElement.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenReturn(expectedReservationMock);
    when(expectedReservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(expectedReservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationElementMock.getReservedTuples()).thenReturn(count);
    when(expectedReservationMock.getReservations())
        .thenReturn(singletonList(reservationElementMock));

    assertEquals(
        expectedReservationMock,
        tuplesDownloadService.getOrCreateReservation(reservationId, tupleType, count));
  }

  @Test
  void
      givenLockedReservationForIdInCache_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    long count = 42;
    Reservation expectedReservationMock = mock(Reservation.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenReturn(expectedReservationMock);
    when(expectedReservationMock.getStatus()).thenReturn(ActivationStatus.LOCKED);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getOrCreateReservation(reservationId, INPUT_MASK_GFP, count));

    assertEquals(
        String.format(NO_RELEASED_TUPLE_RESERVATION_EXCEPTION_MSG, reservationId),
        actualCse.getMessage());
  }

  @Test
  void
      givenCachedReservationForIdMismatchType_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    TupleType mismatchedTupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    Reservation expectedReservationMock = mock(Reservation.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenReturn(expectedReservationMock);
    when(expectedReservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(expectedReservationMock.getTupleType()).thenReturn(mismatchedTupleType);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> tuplesDownloadService.getOrCreateReservation(reservationId, tupleType, count));

    assertEquals(
        String.format(
            RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG,
            reservationId,
            tupleType,
            count,
            expectedReservationMock),
        actualCse.getMessage());
  }

  @Test
  void
      givenCachedReservationForIdMismatchReservedCount_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    TupleType tupleType = INPUT_MASK_GFP;
    long count = 42;
    Reservation expectedReservationMock = mock(Reservation.class);
    ReservationElement reservationElementMock = mock(ReservationElement.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenReturn(expectedReservationMock);
    when(expectedReservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(expectedReservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationElementMock.getReservedTuples()).thenReturn(count - 1);
    when(expectedReservationMock.getReservations())
        .thenReturn(singletonList(reservationElementMock));

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> tuplesDownloadService.getOrCreateReservation(reservationId, tupleType, count));

    assertEquals(
        String.format(
            RESERVATION_DOES_NOT_MATCH_SPECIFICATION_EXCEPTION_MSG,
            reservationId,
            tupleType,
            count,
            expectedReservationMock),
        actualCse.getMessage());
  }

  @Test
  void
      givenNoCachedReservationButCreationThrows_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    CastorClientException expectedException = new CastorClientException("expected");

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(reservationCachingServiceMock.getReservation(reservationId)).thenReturn(null);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenThrow(expectedException);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> tuplesDownloadService.getOrCreateReservation(reservationId, INPUT_MASK_GFP, 42));

    assertEquals(COMMUNICATION_WITH_SLAVES_FAILED_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
  }

  @Test
  void
      givenNoCachedReservationActivationThrows_whenGetOrCreateReservation_thenThrowCastorServiceException() {
    String reservationId = "testReservationId";
    CastorClientException expectedException = new CastorClientException("expected");
    Reservation reservationMock = mock(Reservation.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(reservationCachingServiceMock.getReservation(reservationId)).thenReturn(null);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenReturn(reservationMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(UpdateReservationSupplier.class)))
        .thenThrow(expectedException);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> tuplesDownloadService.getOrCreateReservation(reservationId, INPUT_MASK_GFP, 42));

    assertEquals(COMMUNICATION_WITH_SLAVES_FAILED_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
  }

  @Test
  void
      givenNoCachedReservationAndSuccessfulRequest_whenGetOrCreateReservation_thenReturnNewReservation() {
    String reservationId = "testReservationId";
    Reservation expectedReservationMock = mock(Reservation.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.get()).thenReturn(dedicatedTransactionServiceMock);
    when(reservationCachingServiceMock.getReservation(reservationId)).thenReturn(null);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(CreateReservationSupplier.class)))
        .thenReturn(expectedReservationMock);
    when(dedicatedTransactionServiceMock.runAsNewTransaction(any(UpdateReservationSupplier.class)))
        .thenReturn(expectedReservationMock);

    assertEquals(
        expectedReservationMock,
        tuplesDownloadService.getOrCreateReservation(reservationId, INPUT_MASK_GFP, 42));
  }

  //    @SneakyThrows
  //    @Test
  //    public void
  // givenTuplesCannotBeRetrieved_whenGetTuplesAsSlave_thenThrowCastorServiceException() {
  //        UUID requestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
  //        TupleType tupleType = INPUT_MASK_GFP;
  //        long count = 42;
  //        UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
  //        Reservation reservationMock = mock(Reservation.class);
  //        ReservationElement reservationElementMock = mock(ReservationElement.class);
  //        CastorServiceException expectedException = new CastorServiceException("expected");
  //
  //        when(castorSlaveServicePropertiesMock.getWaitForReservationTimeout()).thenReturn(100L);
  //        when(castorInterVcpClientOptional.isPresent()).thenReturn(false);
  //        when(reservationElementMock.getReservedTuples()).thenReturn(count);
  //        when(reservationElementMock.getStartIndex()).thenReturn(0L);
  //        when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
  //
  // when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
  //        when(tupleStoreMock.downloadTuples(tupleType.getTupleCls(), tupleType.getField(),
  // chunkId, 0, count*tupleType.getTupleSize()))
  //                .thenThrow(expectedException);
  //
  //        try(MockedConstruction<WaitForReservationCallable> awaitReservationMC =
  // mockConstruction(WaitForReservationCallable.class, (mock, context) ->
  //                when(mock.call()).thenReturn(reservationMock))) {
  //
  //            CastorServiceException actualCse = assertThrows(CastorServiceException.class, () ->
  //                    tuplesDownloadService.getTupleList(tupleType.getTupleCls(),
  // tupleType.getField(), count, requestId));
  //
  //            assertEquals(FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG, actualCse.getMessage());
  //            assertEquals(expectedException, actualCse.getCause());
  //        }
  //    }

  @Test
  void givenSuccessfulRequest_whenGetTuplesAsMaster_thenReturnExpectedTuples() throws IOException {
    TupleType tupleType = INPUT_MASK_GFP;
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    long count = 2;
    byte[] tupleData = RandomUtils.nextBytes((int) (tupleType.getTupleSize() * (count)));
    TupleList expectedTupleList =
        TupleList.fromStream(
            tupleType.getTupleCls(),
            tupleType.getField(),
            new ByteArrayInputStream(tupleData),
            tupleData.length);
    UUID requestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    String reservationId = requestId + "_" + tupleType;
    Reservation reservationMock = mock(Reservation.class);
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    TupleChunkMetaDataEntity tupleChunkMetaDataEntityMock = mock(TupleChunkMetaDataEntity.class);

    when(castorInterVcpClientOptional.isPresent()).thenReturn(true);
    when(dedicatedTransactionServiceOptionalMock.isPresent()).thenReturn(true);
    when(reservationCachingServiceMock.getReservation(reservationId)).thenReturn(reservationMock);
    when(reservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(reservationElementMock.getReservedTuples()).thenReturn(count);
    when(reservationElementMock.getStartIndex()).thenReturn(0L);
    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(tupleStoreMock.downloadTuples(
            tupleType.getTupleCls(), tupleType.getField(), chunkId, 0, tupleData.length))
        .thenReturn(expectedTupleList);
    when(tupleChunkMetaDataStorageServiceMock.updateConsumptionForTupleChunkData(chunkId, count))
        .thenReturn(tupleChunkMetaDataEntityMock);
    when(tupleChunkMetaDataEntityMock.getConsumedMarker()).thenReturn(count);
    when(tupleChunkMetaDataEntityMock.getNumberOfTuples()).thenReturn(count);

    assertEquals(
        expectedTupleList,
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    verify(tupleChunkMetaDataStorageServiceMock).forgetTupleChunkData(chunkId);
    verify(tupleStoreMock).deleteTupleChunk(chunkId);
    verify(reservationCachingServiceMock).forgetReservation(reservationId);
  }
}
