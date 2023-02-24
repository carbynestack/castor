/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED;
import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import com.google.common.collect.Lists;
import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.download.DedicatedTransactionService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentRepository;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
@Testcontainers
public class ReservationCachingServiceIT {

  @Container
  public static ReusableRedisContainer reusableRedisContainer =
      ReusableRedisContainer.getInstance();

  @Container
  public static ReusableMinioContainer reusableMinioContainer =
      ReusableMinioContainer.getInstance();

  @Container
  public static ReusablePostgreSQLContainer reusablePostgreSQLContainer =
      ReusablePostgreSQLContainer.getInstance();

  @Autowired private ConsumptionCachingService consumptionCachingService;

  @Autowired private PersistenceTestEnvironment testEnvironment;

  @Autowired private CastorCacheProperties cacheProperties;

  @Autowired private CacheManager cacheManager;

  @Autowired private RedisTemplate<String, Object> redisTemplate;

  @Autowired private TupleChunkFragmentRepository fragmentRepository;

  @Autowired private TupleChunkFragmentStorageService tupleChunkFragmentStorageService;

  @Autowired private TupleChunkFragmentRepository tupleChunkFragmentRepository;

  @Autowired private CastorSlaveServiceProperties castorSlaveServiceProperties;

  @Autowired private DedicatedTransactionService dedicatedTransactionService;

  @Autowired private ApplicationContext applicationContext;

  private ReservationCachingService reservationCachingService;
  private Cache reservationCache;
  private CastorInterVcpClient interVcpClientSpy;

  private final UUID testRequestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
  private final UUID testChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
  private final TupleType testTupleType = MULTIPLICATION_TRIPLE_GFP;
  private final String testReservationId = testRequestId + "_" + testTupleType;
  private final long testNumberReservedTuples = 3;
  private final Reservation testReservation =
      new Reservation(
          testReservationId,
          testTupleType,
          singletonList(new ReservationElement(testChunkId, testNumberReservedTuples, 0)));

  @BeforeEach
  public void setUp() {
    if (reservationCache == null) {
      reservationCache = cacheManager.getCache(cacheProperties.getReservationStore());
    }
    if (interVcpClientSpy == null) {
      interVcpClientSpy = spy(applicationContext.getBean(CastorInterVcpClient.class));
    }
    reservationCachingService =
        new ReservationCachingService(
            cacheProperties,
            consumptionCachingService,
            redisTemplate,
            tupleChunkFragmentStorageService,
            castorSlaveServiceProperties,
            Optional.of(interVcpClientSpy),
            Optional.of(dedicatedTransactionService));
    testEnvironment.clearAllData();
  }

  @Test
  void givenCacheIsEmpty_whenGetReservation_thenReturnNull() {
    assertNull(
        reservationCachingService.getUnlockedReservation(
            testReservation.getReservationId(), testTupleType, testNumberReservedTuples));
  }

  @Test
  void givenReservationInCache_whenGetReservation_thenKeepReservationUntouchedInCache() {
    reservationCache.put(testReservation.getReservationId(), testReservation.setStatus(UNLOCKED));
    assertEquals(
        testReservation,
        reservationCachingService.getUnlockedReservation(
            testReservation.getReservationId(), testTupleType, testNumberReservedTuples));
    assertEquals(
        testReservation,
        reservationCachingService.getUnlockedReservation(
            testReservation.getReservationId(), testTupleType, testNumberReservedTuples));
  }

  @Test
  void givenSharingReservationFails_whenCreateReservation_thenRollbackFragmentation() {
    TupleType requestedTupleType = MULTIPLICATION_TRIPLE_GFP;
    long requestedNoTuples = 12;
    UUID requestId = UUID.fromString("a345f933-bf70-4c7a-b6cd-312b55a6ff9c");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    String resultingReservationId = requestId + "_" + requestedTupleType;
    long fragmentStartIndex = 0;
    long fragmentLength = 2 * requestedNoTuples;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            chunkId, requestedTupleType, fragmentStartIndex, fragmentLength, UNLOCKED, null);
    String expectedReservationId = requestId + "_" + requestedTupleType;
    ReservationElement expectedReservationElement =
        new ReservationElement(chunkId, requestedNoTuples, fragmentStartIndex);
    Reservation expectedReservation =
        new Reservation(
            expectedReservationId, requestedTupleType, singletonList(expectedReservationElement));
    CastorServiceException expectedException =
        new CastorServiceException("sharing Reservation failed");

    tupleChunkFragmentRepository.save(existingFragment);

    doThrow(expectedException).when(interVcpClientSpy).shareReservation(expectedReservation);

    CastorServiceException actualCSE =
        assertThrows(
            CastorServiceException.class,
            () ->
                reservationCachingService.createReservation(
                    resultingReservationId, testTupleType, requestedNoTuples));

    assertEquals(expectedException, actualCSE);

    assertEquals(singletonList(existingFragment), tupleChunkFragmentRepository.findAll());
  }

  @Test
  void givenSuccessfulRequest_whenForgetReservation_thenRemoveFromCache() {
    reservationCache.put(testReservation.getReservationId(), testReservation);
    assertEquals(
        testReservation,
        reservationCache.get(testReservation.getReservationId(), Reservation.class));
    reservationCachingService.forgetReservation(testReservation.getReservationId());
    assertNull(reservationCache.get(testReservation.getReservationId(), Reservation.class));
  }

  @Test
  void whenReferencedSequenceIsSplitInFragments_whenApplyReservation_thenApplyAccordingly() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement re =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
    Reservation r = new Reservation(reservationId, tupleType, singletonList(re));

    TupleChunkFragmentEntity fragmentBefore =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, 0, requestedStartIndex, UNLOCKED, null);
    TupleChunkFragmentEntity fragmentPart1 =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex,
            requestedStartIndex + requestedLength - 5,
            UNLOCKED,
            null);
    TupleChunkFragmentEntity fragmentContainingRest =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex + requestedLength - 5,
            Long.MAX_VALUE,
            UNLOCKED,
            null);

    fragmentBefore = fragmentRepository.save(fragmentBefore);
    fragmentPart1 = fragmentRepository.save(fragmentPart1);
    fragmentContainingRest = fragmentRepository.save(fragmentContainingRest);

    reservationCachingService.keepAndApplyReservation(r);
    List<TupleChunkFragmentEntity> actualFragments =
        Lists.newArrayList(fragmentRepository.findAll());
    // fragment remains unchanged since it does not hold any tuples of interest
    assertTrue(actualFragments.remove(fragmentBefore));
    // fragment which is of interest as it is gets reserved
    assertTrue(actualFragments.remove(fragmentPart1.setReservationId(reservationId)));
    // fragment which contains some tuples of interest gets split and first part is reserved
    assertTrue(
        actualFragments.remove(
            fragmentContainingRest
                .setEndIndex(requestedStartIndex + requestedLength)
                .setReservationId(reservationId)));
    // remnant of previous fragment got created and is available
    assertEquals(1, actualFragments.size());
    TupleChunkFragmentEntity lastFragment = actualFragments.get(0);
    assertEquals(tupleChunkId, lastFragment.getTupleChunkId());
    assertEquals(tupleType, lastFragment.getTupleType());
    assertEquals(Long.MAX_VALUE, lastFragment.getEndIndex());
    assertEquals(requestedStartIndex + requestedLength, lastFragment.getStartIndex());
    assertEquals(UNLOCKED, lastFragment.getActivationStatus());
    assertNull(lastFragment.getReservationId());
    Set<String> keysInCache =
        redisTemplate.keys(
            CacheKeyPrefix.simple()
                    .compute(
                        cacheProperties.getConsumptionStorePrefix()
                            + testReservation.getTupleType())
                + "*");
    assertEquals(1, keysInCache.size());
    assertEquals(
        requestedLength,
        consumptionCachingService.getConsumptionForTupleType(Long.MIN_VALUE, tupleType));
  }
}
