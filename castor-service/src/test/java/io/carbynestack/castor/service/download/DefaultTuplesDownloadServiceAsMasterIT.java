/*
 * Copyright (c) 2024 - for information on the respective copyright owner
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
import static org.junit.jupiter.api.Assertions.*;
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
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.util.Lists;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
@Testcontainers
public class DefaultTuplesDownloadServiceAsMasterIT {
  @Container
  public static ReusableRedisContainer reusableRedisContainer =
      ReusableRedisContainer.getInstance();

  @Container
  public static ReusableMinioContainer reusableMinioContainer =
      ReusableMinioContainer.getInstance();

  @Container
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

  @BeforeEach
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
  void givenSharingReservationFails_whenGetTuples_thenDoNotRollbackReservation() {
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
            null,
            false);

    String expectedReservationId = requestId + "_" + requestedTupleType;

    TupleChunkFragmentEntity resultingSplitFragment =
        spy(
            TupleChunkFragmentEntity.of(
                chunkId,
                requestedTupleType,
                requestedNoTuples,
                fragmentLength,
                ActivationStatus.UNLOCKED,
                null,
                false));
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, requestedNoTuples, fragmentStartIndex);
    Reservation expectedReservation =
        new Reservation(
            expectedReservationId, requestedTupleType, singletonList(expectedReservationElement));

    tupleChunkFragmentRepository.save(existingFragment);
    // needs to be set like this because 'id' is a generated sequential value
    when(resultingSplitFragment.getId()).thenReturn(existingFragment.getId() + 1);
    doReturn(false).when(interVcpClientMock).shareReservation(expectedReservation);
    existingFragment.setReservationId(expectedReservationId);

    CastorServiceException actualCSE =
        assertThrows(
            CastorServiceException.class,
            () ->
                tuplesDownloadService.getTupleList(
                    requestedTupleType.getTupleCls(),
                    requestedTupleType.getField(),
                    requestedNoTuples,
                    requestId));
    existingFragment.setEndIndex(requestedNoTuples);
    assertEquals(SHARING_RESERVATION_FAILED_EXCEPTION_MSG, actualCSE.getMessage());

    assertTrue(
        Arrays.asList(resultingSplitFragment, existingFragment)
            .containsAll(
                StreamSupport.stream(tupleChunkFragmentRepository.findAll().spliterator(), false)
                    .collect(Collectors.toList())));
    assertEquals(12, consumptionCachingService.getConsumptionForTupleType(0, requestedTupleType));
    assertEquals(reservationCache.get(expectedReservationId).get(), expectedReservation);
  }

  @Test
  void givenRetrievingTuplesFails_whenGetTuples_thenKeepReservationAndReservationMarker() {
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
            null,
            true);
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
            expectedReservationId,
            true);
    TupleChunkFragmentEntity expectedNewFragment =
        TupleChunkFragmentEntity.of(
            chunkId,
            tupleType,
            fragmentStartIndex + count,
            fragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null,
            true);

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
        expectedReservation.setStatus(ActivationStatus.LOCKED),
        reservationCache.get(expectedReservationId).get());
    assertEquals(count, consumptionCachingService.getConsumptionForTupleType(0, tupleType));
    verify(tupleStoreSpy, never()).deleteTupleChunk(any(UUID.class));
  }

  @SneakyThrows
  @Test
  void
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
            null,
            true);
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

    byte[] tupleList =
        tuplesDownloadService.getTupleList(
            tupleType.getTupleCls(), tupleType.getField(), count, requestId);

    assertArrayEquals(
        TupleList.fromStream(
                tupleType.getTupleCls(),
                tupleType.getField(),
                new ByteArrayInputStream(
                    tupleData,
                    (int) (fragmentStartIndex * tupleType.getTupleSize()),
                    (int) ((fragmentStartIndex + count) * tupleType.getTupleSize())),
                count * tupleType.getTupleSize())
            .toByteArray(),
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
