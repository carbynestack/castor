/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static io.carbynestack.castor.service.download.DefaultTuplesDownloadService.FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.cache.CreateReservationSupplier.SHARING_RESERVATION_FAILED_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.tuplestore.MinioTupleStore.ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.MinioProperties;
import io.carbynestack.castor.service.persistence.cache.ConsumptionCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentRepository;
import io.carbynestack.castor.service.persistence.tuplestore.MinioTupleStore;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import io.carbynestack.castor.service.util.TupleChunkFragmentEntityListMatcher;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.util.Lists;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
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

  @Autowired private ConsumptionCachingService consumptionCachingService;

  @Autowired private TupleChunkFragmentRepository tupleChunkFragmentRepository;

  @Autowired private MinioClient minioClient;

  @Autowired private MinioProperties minioProperties;

  @MockBean private CastorInterVcpClient interVcpClientMock;

  @SpyBean private MinioTupleStore tupleStoreSpy;

  @Autowired private DefaultTuplesDownloadService tuplesDownloadService;

  private Cache reservationCache;
  private Cache multiplicationTripleTelemetryCache;

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
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

  @Test
  public void givenSharingReservationFails_whenGetTuples_thenRollbackReservation() {
    TupleType requestedTupleType = MULTIPLICATION_TRIPLE_GFP;
    long requestedNoTuples = 12;
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    long fragmentStartIndex = 0;
    long fragmentLength = 2 * requestedNoTuples;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            requestedTupleType,
            fragmentStartIndex,
            fragmentLength,
            ActivationStatus.UNLOCKED,
            null);
    String expectedReservationId = requestId + "_" + requestedTupleType;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, requestedNoTuples, fragmentStartIndex);
    Reservation expectedReservation =
        new Reservation(
            expectedReservationId, requestedTupleType, singletonList(expectedReservationElement));

    tupleChunkFragmentRepository.save(existingFragment);
    doReturn(false).when(interVcpClientMock).shareReservation(expectedReservation);

    CastorServiceException actualCSE =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    requestedTupleType.getTupleCls(),
                    requestedTupleType.getField(),
                    requestedNoTuples,
                    requestId));

    assertEquals(SHARING_RESERVATION_FAILED_EXCEPTION_MSG, actualCSE.getMessage());

    assertEquals(singletonList(existingFragment), tupleChunkFragmentRepository.findAll());
    assertEquals(0, consumptionCachingService.getConsumptionForTupleType(0, requestedTupleType));
    assertNull(reservationCache.get(expectedReservationId));
  }

  @Test
  public void
      givenRetrievingTuplesFails_whenGetTuples_thenKeepReservationAndReservationMarkerButConsumptionMarkerRemainsUntouched() {
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    long fragmentStartIndex = 2;
    long fragmentEndIndex = fragmentStartIndex + 2 * count;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            tupleType,
            fragmentStartIndex,
            fragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null);
    String expectedReservationId = requestId + "_" + tupleType;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, count, fragmentStartIndex);
    Reservation expectedReservation =
        new Reservation(
            expectedReservationId, tupleType, singletonList(expectedReservationElement));
    TupleChunkFragmentEntity expectedReservedFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            tupleType,
            fragmentStartIndex,
            fragmentStartIndex + count,
            ActivationStatus.UNLOCKED,
            expectedReservationId);
    TupleChunkFragmentEntity expectedNewFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            tupleType,
            fragmentStartIndex + count,
            fragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null);

    tupleChunkFragmentRepository.save(existingFragment);

    doReturn(true).when(interVcpClientMock).shareReservation(expectedReservation);

    // call will fail as the tuple chunk referenced by the fragment cannot be found in the database
    RuntimeException actualException =
        assertThrows(
            RuntimeException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    tupleType.getTupleCls(), tupleType.getField(), count, requestId));

    assertEquals(FAILED_RETRIEVING_TUPLES_EXCEPTION_MSG, actualException.getMessage());
    assertEquals(ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG, actualException.getCause().getMessage());

    MatcherAssert.assertThat(
        Lists.newArrayList(tupleChunkFragmentRepository.findAll()),
        TupleChunkFragmentEntityListMatcher.containsAll(
            expectedNewFragment, expectedReservedFragment));
    assertEquals(
        expectedReservation.setStatus(ActivationStatus.UNLOCKED),
        reservationCache.get(expectedReservationId).get());
    assertEquals(count, consumptionCachingService.getConsumptionForTupleType(0, tupleType));
    verify(tupleStoreSpy, never()).deleteTupleChunk(any(UUID.class));
  }

  @SneakyThrows
  @Test
  public void
      givenSuccessfulRequestAndLastTuplesConsumed_whenGetTuples_thenReturnTuplesAndCleanupAccordingly() {
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    long count = 42;
    long fragmentStartIndex = 2;
    long fragmentEndIndex = fragmentStartIndex + count;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            tupleType,
            fragmentStartIndex,
            fragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null);
    String expectedReservationId = requestId + "_" + tupleType;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, count, fragmentStartIndex);
    Reservation expectedReservation =
        new Reservation(
            expectedReservationId, tupleType, singletonList(expectedReservationElement));
    byte[] tupleData = RandomUtils.nextBytes((int) (tupleType.getTupleSize() * fragmentEndIndex));
    TupleChunk existingTupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    try (InputStream inputStream = new ByteArrayInputStream(tupleData)) {
      int size = existingTupleChunk.getNumberOfTuples() * tupleType.getTupleSize();
      minioClient.putObject(
          PutObjectArgs.builder()
              .bucket(minioProperties.getBucket())
              .object(chunkId.toString())
              .stream(inputStream, size, -1)
              .build());
    }

    tupleChunkFragmentRepository.save(existingFragment);

    doReturn(true).when(interVcpClientMock).shareReservation(expectedReservation);

    TupleList tupleList =
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId);

    assertEquals(
        TupleList.fromStream(
            tupleType.getTupleCls(),
            tupleType.getField(),
            new ByteArrayInputStream(
                tupleData,
                (int) (fragmentStartIndex * tupleType.getTupleSize()),
                (int) ((fragmentStartIndex + count) * tupleType.getTupleSize())),
            count * tupleType.getTupleSize()),
        tupleList);

    // no fragments stored -> existing fragment was reserved, consumed and then deleted
    assertFalse(tupleChunkFragmentRepository.findAll().iterator().hasNext());
    // reservation was deleted after consumption
    assertNull(reservationCache.get(expectedReservationId));
    assertEquals(count, consumptionCachingService.getConsumptionForTupleType(0, tupleType));
    // no tuple chunks stored -> existing chunk was removed after all linked fragments were consumed
    assertFalse(
        minioClient
            .listObjects(ListObjectsArgs.builder().bucket(minioProperties.getBucket()).build())
            .iterator()
            .hasNext());
  }
}
