/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static io.carbynestack.castor.service.download.CreateReservationSupplier.FAILED_RESERVE_AMOUNT_TUPLES_EXCEPTION_MSG;
import static io.carbynestack.castor.service.download.CreateReservationSupplier.SHARING_RESERVATION_FAILED_EXCEPTION_MSG;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.config.MinioProperties;
import io.carbynestack.castor.service.persistence.cache.ConsumptionCachingService;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataEntity;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetadataRepository;
import io.carbynestack.castor.service.persistence.tuplestore.MinioTupleStore;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.util.IterableUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
public class DefaultTuplesDownloadServiceAsMasterIT {
  @ClassRule
  public static ReusableRedisContainer reusableRedisContainer =
      ReusableRedisContainer.getInstance();

  @ClassRule
  public static ReusableMinioContainer reusableMinioContainer =
      ReusableMinioContainer.getInstance();

  @ClassRule
  public static ReusablePostgreSQLContainer reusablePostgreSQLContainer =
      ReusablePostgreSQLContainer.getInstance();

  @Autowired private PersistenceTestEnvironment testEnvironment;

  @Autowired private CastorCacheProperties cacheProperties;

  @Autowired private CacheManager cacheManager;

  @Autowired private ApplicationContext applicationContext;

  @Autowired private ConsumptionCachingService consumptionCachingService;

  @Autowired private TupleChunkMetadataRepository tupleChunkMetadataRepository;

  @Autowired private MinioClient minioClient;

  @Autowired private MinioProperties minioProperties;

  @Autowired private CastorSlaveServiceProperties slaveServiceProperties;

  @Autowired private DedicatedTransactionService dedicatedTransactionService;

  private MinioTupleStore tupleStoreSpy;
  private TupleChunkMetaDataStorageService tupleChunkMetaDataStorageServiceSpy;
  private ReservationCachingService reservationCachingServiceSpy;
  private DefaultTuplesDownloadService tuplesDownloadService;
  private CastorInterVcpClient interVcpClientSpy;

  private Cache reservationCache;
  private Cache multiplicationTripleTelemetryCache;

  @Before
  public void setUp() {
    initialize();
    if (reservationCache == null) {
      reservationCache =
          Objects.requireNonNull(cacheManager.getCache(cacheProperties.getReservationStore()));
    }
    if (multiplicationTripleTelemetryCache == null) {
      multiplicationTripleTelemetryCache =
          Objects.requireNonNull(
              cacheManager.getCache(
                  cacheProperties.getConsumptionStorePrefix() + MULTIPLICATION_TRIPLE_GFP));
    }
    testEnvironment.clearAllData();
  }

  /**
   * We were facing multiple issues where {@link SpyBean}s were not successfully registered as spies
   * and caused issues when verifying access ({@link
   * org.mockito.exceptions.misusing.NotAMockException}). To workaround these issues we're now
   * explicitly spying the manually accessed beans and inject these in a dedicated test {@link
   * DefaultTuplesDownloadService}.
   */
  private void initialize() {
    System.out.println("Initialize");
    tupleStoreSpy = spy(new MinioTupleStore(minioClient, minioProperties));
    tupleChunkMetaDataStorageServiceSpy =
        spy(new TupleChunkMetaDataStorageService(tupleChunkMetadataRepository));
    reservationCachingServiceSpy = spy(applicationContext.getBean(ReservationCachingService.class));
    interVcpClientSpy = spy(applicationContext.getBean(CastorInterVcpClient.class));

    tuplesDownloadService =
        new DefaultTuplesDownloadService(
            tupleStoreSpy,
            tupleChunkMetaDataStorageServiceSpy,
            reservationCachingServiceSpy,
            slaveServiceProperties,
            Optional.of(interVcpClientSpy),
            Optional.of(dedicatedTransactionService));
  }

  @Test
  public void givenPersistingReservationFails_whenGetTuples_thenRollbackReservedMarkers() {
    RuntimeException expectedException = new RuntimeException("expected");
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    final TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    TupleChunkMetaDataEntity initialMetaData =
        TupleChunkMetaDataEntity.of(chunkId, tupleType, count).setStatus(ActivationStatus.UNLOCKED);
    tupleChunkMetadataRepository.save(initialMetaData);

    doThrow(expectedException).when(reservationCachingServiceSpy).keepReservation(any());

    RuntimeException actualException =
        assertThrows(
            RuntimeException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    MultiplicationTriple.class, Field.GFP, count, requestId));

    assertEquals(expectedException, actualException);
    assertEquals(
        singletonList(initialMetaData),
        IterableUtil.nonNullElementsIn(tupleChunkMetadataRepository.findAll()));
    assertEquals(
        0, consumptionCachingService.getConsumptionForTupleType(0, MULTIPLICATION_TRIPLE_GFP));
  }

  @Test
  public void givenMultipleChunksButNotEnoughTuples_whenGetTuples_thenRollbackAllMarkers() {
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId1 = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    UUID chunkId2 = UUID.fromString("0cb02fab-b951-4947-bed5-89fbe2f9581e");
    final TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    TupleChunkMetaDataEntity chunk1MetaData =
        TupleChunkMetaDataEntity.of(chunkId1, tupleType, count / 3)
            .setStatus(ActivationStatus.UNLOCKED);
    TupleChunkMetaDataEntity chunk2MetaData =
        TupleChunkMetaDataEntity.of(chunkId2, tupleType, count / 3)
            .setStatus(ActivationStatus.UNLOCKED);
    tupleChunkMetadataRepository.save(chunk1MetaData);
    tupleChunkMetadataRepository.save(chunk2MetaData);

    doReturn(count).when(tupleChunkMetaDataStorageServiceSpy).getAvailableTuples(tupleType);

    CastorServiceException actualException =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    MultiplicationTriple.class, Field.GFP, count, requestId));

    assertEquals(
        String.format(
            FAILED_RESERVE_AMOUNT_TUPLES_EXCEPTION_MSG,
            tupleType,
            chunk1MetaData.getNumberOfTuples() + chunk2MetaData.getNumberOfTuples(),
            count),
        actualException.getMessage());
    assertEquals(chunk1MetaData, tupleChunkMetadataRepository.findById(chunkId1).get());
    assertEquals(chunk2MetaData, tupleChunkMetadataRepository.findById(chunkId2).get());
  }

  @Ignore("Transactions are currently disabled for cache operations")
  @Test
  public void
      givenSharingReservationFails_whenGetTuples_thenRollbackReservationAndReservedMarkerUpdates() {
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    TupleChunkMetaDataEntity initialMetaData =
        TupleChunkMetaDataEntity.of(chunkId, tupleType, count).setStatus(ActivationStatus.UNLOCKED);
    tupleChunkMetadataRepository.save(initialMetaData);

    doReturn(false).when(interVcpClientSpy).shareReservation(any(Reservation.class));

    CastorServiceException actualException =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    assertEquals(SHARING_RESERVATION_FAILED_EXCEPTION_MSG, actualException.getMessage());
    assertNull(reservationCache.get(requestId + "_" + tupleType));
    assertEquals(
        singletonList(initialMetaData),
        IterableUtil.toCollection(tupleChunkMetadataRepository.findAll()));
    assertEquals(
        0, consumptionCachingService.getConsumptionForTupleType(0, MULTIPLICATION_TRIPLE_GFP));
  }

  @Test
  public void
      givenRetrievingTuplesFails_whenGetTuples_thenKeepReservationAndReservationMarkerButRemainConsumptionMarkerUntouched() {
    RuntimeException expectedException = new RuntimeException("expected");
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    long alreadyReserved = 2;
    long alreadyConsumed = 1;
    TupleChunkMetaDataEntity initialMetaData =
        TupleChunkMetaDataEntity.of(chunkId, tupleType, count + alreadyReserved)
            .setReservedMarker(alreadyReserved)
            .setConsumedMarker(alreadyConsumed)
            .setStatus(ActivationStatus.UNLOCKED);
    tupleChunkMetadataRepository.save(initialMetaData);

    doReturn(true).when(interVcpClientSpy).shareReservation(any(Reservation.class));
    doNothing()
        .when(interVcpClientSpy)
        .updateReservationStatus(anyString(), any(ActivationStatus.class));
    doThrow(expectedException)
        .when(tupleStoreSpy)
        .downloadTuples(any(), any(Field.class), any(UUID.class), anyLong(), anyLong());

    RuntimeException actualException =
        assertThrows(
            RuntimeException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    assertEquals(expectedException, actualException);
    Reservation actualCreatedReservation =
        reservationCache.get(requestId + "_" + tupleType, Reservation.class);
    assertNotNull(actualCreatedReservation);
    assertEquals(ActivationStatus.UNLOCKED, actualCreatedReservation.getStatus());
    assertEquals(tupleType, actualCreatedReservation.getTupleType());
    assertEquals(1, actualCreatedReservation.getReservations().size());
    ReservationElement actualReservationElement = actualCreatedReservation.getReservations().get(0);
    assertEquals(chunkId, actualReservationElement.getTupleChunkId());
    assertEquals(alreadyReserved, actualReservationElement.getStartIndex());
    assertEquals(count, actualReservationElement.getReservedTuples());
    assertEquals(
        count, consumptionCachingService.getConsumptionForTupleType(0, MULTIPLICATION_TRIPLE_GFP));
    assertEquals(1, IterableUtil.toCollection(tupleChunkMetadataRepository.findAll()).size());
    TupleChunkMetaDataEntity actualMetadata = tupleChunkMetadataRepository.findById(chunkId).get();
    assertEquals(initialMetaData.getNumberOfTuples(), actualMetadata.getNumberOfTuples());
    assertEquals(initialMetaData.getTupleType(), actualMetadata.getTupleType());
    assertEquals(count + alreadyReserved, actualMetadata.getReservedMarker());
    assertEquals(initialMetaData.getConsumedMarker(), actualMetadata.getConsumedMarker());
  }

  @SneakyThrows
  @Test
  public void
      givenSuccessfulRequestAndLastTuplesConsumed_whenGetTuples_thenReturnTuplesAndCleanupAccordingly() {
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    String expectedReservationId = requestId + "_" + tupleType;
    long count = 42;
    TupleChunkMetaDataEntity initialMetaData =
        TupleChunkMetaDataEntity.of(chunkId, tupleType, count).setStatus(ActivationStatus.UNLOCKED);
    tupleChunkMetadataRepository.save(initialMetaData);

    byte[] tupleData = RandomUtils.nextBytes((int) (tupleType.getTupleSize() * (count)));
    TupleChunk expectedTupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    try (InputStream inputStream = new ByteArrayInputStream(tupleData)) {
      int size = expectedTupleChunk.getNumberOfTuples() * tupleType.getTupleSize();
      minioClient.putObject(
          PutObjectArgs.builder()
              .bucket(minioProperties.getBucket())
              .object(chunkId.toString())
              .stream(inputStream, size, -1)
              .contentType("ByteArray")
              .build());
    }

    doReturn(true).when(interVcpClientSpy).shareReservation(any(Reservation.class));
    doNothing()
        .when(interVcpClientSpy)
        .updateReservationStatus(anyString(), any(ActivationStatus.class));

    TupleList tupleList =
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId);

    assertEquals(expectedTupleChunk, tupleList.asChunk(chunkId));
    assertNull(reservationCache.get(expectedReservationId, Reservation.class));
    assertEquals(Optional.empty(), tupleChunkMetadataRepository.findById(chunkId));
    assertThrows(
        ErrorResponseException.class,
        () ->
            minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(minioProperties.getBucket())
                    .object(chunkId.toString())
                    .build()));
    assertEquals(
        count, consumptionCachingService.getConsumptionForTupleType(0, MULTIPLICATION_TRIPLE_GFP));

    ArgumentCaptor<Reservation> reservationArgumentCaptor =
        ArgumentCaptor.forClass(Reservation.class);
    verify(reservationCachingServiceSpy).keepReservation(reservationArgumentCaptor.capture());
    Reservation actualReservation = reservationArgumentCaptor.getValue();
    assertEquals(expectedReservationId, actualReservation.getReservationId());
    assertEquals(1, actualReservation.getReservations().size());
    ReservationElement actualReservationElement = actualReservation.getReservations().get(0);
    assertEquals(count, actualReservationElement.getReservedTuples());
    assertEquals(0, actualReservationElement.getStartIndex());
    assertEquals(chunkId, actualReservationElement.getTupleChunkId());
    verify(reservationCachingServiceSpy)
        .updateReservation(expectedReservationId, ActivationStatus.UNLOCKED);
  }
}
