/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED;
import static io.carbynestack.castor.common.entities.Field.GF2N;
import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import com.google.common.collect.Lists;
import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import io.carbynestack.castor.service.download.DedicatedTransactionService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentRepository;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import java.util.*;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
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

  @Autowired @SpyBean private TupleChunkFragmentRepository tupleChunkFragmentRepository;

  @Autowired private CastorSlaveServiceProperties castorSlaveServiceProperties;

  @Autowired private DedicatedTransactionService dedicatedTransactionService;

  @Autowired private ApplicationContext applicationContext;

  @Autowired private CastorServiceProperties castorServiceProperties;

  private ReservationCachingService reservationCachingService;
  private Cache reservationCache;
  private CastorInterVcpClient interVcpClientSpy;
  private TupleChunkFragmentRepository fragmentRepositorySpy;

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
            castorServiceProperties,
            Optional.of(interVcpClientSpy),
            Optional.of(dedicatedTransactionService));
    testEnvironment.clearAllData();
  }

  @Test
  void testComposing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    UUID tupleChunkId2 = UUID.randomUUID();
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    TupleType tupleType2 = TupleType.INPUT_MASK_GF2N;
    byte[] expectedMGFPTupleData =
        RandomUtils.nextBytes( // Value = elemsize * arity --> *2 for value + MAC
            GFP.getElementSize() * tupleType.getTupleSize() * 700000);

    byte[] expectedMGF2nTupleData =
        RandomUtils.nextBytes( // Value = elemsize * arity --> *2 for value + MAC
            GF2N.getElementSize() * tupleType2.getTupleSize() * 50000);

    TupleChunk mGfpTupleChunk =
        TupleChunk.of(
            tupleType.getTupleCls(), tupleType.getField(), tupleChunkId, expectedMGFPTupleData);
    TupleChunk mGf2nTupleChunk =
        TupleChunk.of(
            tupleType2.getTupleCls(), tupleType2.getField(), tupleChunkId2, expectedMGF2nTupleData);

    Answer shareReservationAnswer =
        (Answer)
            invocation -> {
              reservationCachingService.keepAndApplyReservation(
                  invocation.getArgument(0, Reservation.class));
              return true;
            };

    doReturn(true).when(interVcpClientSpy).shareReservation(isA(Reservation.class));
    //    doAnswer(shareReservationAnswer)
    //        .when(interVcpClientSpy)
    //        .shareReservation(isA(Reservation.class));

    Answer mockFindFragmentAnswer =
        (Answer)
            invocation -> {
              return tupleChunkFragmentRepository
                  .mockFindAvailableFragmentForTupleChunkContainingIndex(
                      invocation.getArgument(0, UUID.class), invocation.getArgument(1, Long.class));
            };
    doAnswer(mockFindFragmentAnswer)
        .when(tupleChunkFragmentRepository)
        .findAvailableFragmentForTupleChunkContainingIndex(isA(UUID.class), isA(Long.class));

    Answer mockSave =
        (Answer)
            invocation -> {
              return invocation.getArgument(0, TupleChunkFragmentEntity.class);
            };
    // doAnswer(mockSave).when(tupleChunkFragmentRepository).save(isA(TupleChunkFragmentEntity.class));

    Answer mockReserveRoundFragmentsByIndices =
        (Answer)
            invocation -> {
              return tupleChunkFragmentRepository.mockReserveRoundFragmentsByIndices(
                  invocation.getArgument(0, ArrayList.class),
                  invocation.getArgument(1, String.class),
                  tupleChunkId);
            };
    doAnswer(mockReserveRoundFragmentsByIndices)
        .when(tupleChunkFragmentRepository)
        .reserveRoundFragmentsByIndices(isA(ArrayList.class), isA(String.class), isA(UUID.class));

    Answer mockReserveRoundTuples =
        (Answer)
            invocation -> {
              return tupleChunkFragmentRepository.mockRetrieveAndReserveRoundFragmentsByType(
                  invocation.getArgument(0, String.class),
                  invocation.getArgument(1, Integer.class),
                  invocation.getArgument(2, String.class));
            };
    lenient()
        .doAnswer(mockReserveRoundTuples)
        .when(tupleChunkFragmentRepository)
        .retrieveAndReserveRoundFragmentsByType(isA(String.class), anyInt(), isA(String.class));

    Answer mockRetrieveSinglePartialFragment =
        (Answer)
            invocation -> {
              return tupleChunkFragmentRepository.mockRetrieveSinglePartialFragment(
                  invocation.getArgument(0, String.class));
            };
    lenient()
        .doAnswer(mockRetrieveSinglePartialFragment)
        .when(tupleChunkFragmentRepository)
        .retrieveSinglePartialFragmentPreferSmall(isA(String.class));

    Answer mocklockFirstTupleReturningReservationId =
        (Answer)
            invocation -> {
              return tupleChunkFragmentRepository.lockFirstFragmentReturningReservationId(
                  invocation.getArgument(0, UUID.class), invocation.getArgument(1, Long.class));
            };

    // doReturn(21).when(tupleChunkFragmentRepository).lockTuplesWithoutRetrievingForConsumption(isA(String.class), isA(UUID.class), anyLong());

    // Answer mockGetFragments = (Answer) invocation -> { return
    // tupleChunkFragmentRepository.mockGetAllByReservationId(invocation.getArgument(0,
    // String.class));};
    // doAnswer(mockGetFragments).when(tupleChunkFragmentRepository).deleteByChunkAndStartIndex(any(), any());
    List<TupleChunkFragmentEntity> fragmentEntities = generateFragmentsForChunk(mGfpTupleChunk);

    List<TupleChunkFragmentEntity> fragmentEntities1 = generateFragmentsForChunk(mGf2nTupleChunk);

    // tupleChunkFragmentStorageService.addUniqueConstraint();

    // fragmentStorageService.keep(fragmentEntities1.remove(0));
    tupleChunkFragmentStorageService.keepRound(fragmentEntities1);
    tupleChunkFragmentStorageService.keepRound(fragmentEntities);

    tupleChunkFragmentRepository.unlockAllForTupleChunk(tupleChunkId);
    tupleChunkFragmentRepository.unlockAllForTupleChunk(tupleChunkId2);

    Reservation res =
        reservationCachingService.createReservation(testReservationId, tupleType, 600000);
    assertEquals(600, res.getReservations().size());
  }

  protected ArrayList<TupleChunkFragmentEntity> generateFragmentsForChunk(TupleChunk tupleChunk) {
    ArrayList<TupleChunkFragmentEntity> fragments = new ArrayList<>();
    for (long i = 0;
        i * castorServiceProperties.getInitialFragmentSize() < tupleChunk.getNumberOfTuples();
        i++) {
      fragments.add(
          TupleChunkFragmentEntity.of(
              tupleChunk.getChunkId(),
              tupleChunk.getTupleType(),
              i * castorServiceProperties.getInitialFragmentSize(),
              Math.min(
                  (i + 1) * castorServiceProperties.getInitialFragmentSize(),
                  tupleChunk.getNumberOfTuples())));
    }
    return fragments;
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

  @Disabled
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
            chunkId, requestedTupleType, fragmentStartIndex, fragmentLength, UNLOCKED, null, false);
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
  @SneakyThrows
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
            tupleChunkId, tupleType, 0, requestedStartIndex, UNLOCKED, null, true);
    TupleChunkFragmentEntity fragmentPart1 =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex,
            requestedStartIndex + requestedLength - 5,
            UNLOCKED,
            null,
            true);
    TupleChunkFragmentEntity fragmentContainingRest =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex + requestedLength - 5,
            Long.MAX_VALUE,
            UNLOCKED,
            null,
            true);

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
