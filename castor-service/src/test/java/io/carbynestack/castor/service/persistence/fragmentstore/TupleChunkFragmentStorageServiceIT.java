/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.common.entities.ActivationStatus.LOCKED;
import static io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED;
import static io.carbynestack.castor.common.entities.Field.GF2N;
import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService.CONFLICT_EXCEPTION_MSG;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorServiceProperties;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
@Testcontainers
public class TupleChunkFragmentStorageServiceIT {

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
  @Autowired private TupleChunkFragmentRepository fragmentRepository;
  @Autowired private TupleChunkFragmentStorageService fragmentStorageService;
  @Autowired private CastorServiceProperties castorServiceProperties;

  @BeforeEach
  public void setUp() {
    testEnvironment.clearAllData();
  }

  @Test
  void testFragmentCache() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    UUID tupleChunkId2 = UUID.randomUUID();
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    TupleType tupleType2 = TupleType.MULTIPLICATION_TRIPLE_GF2N;
    byte[] expectedMGFPTupleData =
        RandomUtils.nextBytes( // Value = elemsize * arity --> *2 for value + MAC
            GFP.getElementSize() * TupleType.MULTIPLICATION_TRIPLE_GFP.getArity() * 50000 * 2);

    byte[] expectedMGF2nTupleData =
        RandomUtils.nextBytes( // Value = elemsize * arity --> *2 for value + MAC
            GF2N.getElementSize() * TupleType.MULTIPLICATION_TRIPLE_GF2N.getArity() * 50000 * 2);

    TupleChunk mGfpTupleChunk =
        TupleChunk.of(
            tupleType.getTupleCls(), tupleType.getField(), tupleChunkId, expectedMGFPTupleData);
    TupleChunk mGf2nTupleChunk =
        TupleChunk.of(
            tupleType2.getTupleCls(), tupleType2.getField(), tupleChunkId2, expectedMGF2nTupleData);

    List<TupleChunkFragmentEntity> fragmentEntities = generateFragmentsForChunk(mGfpTupleChunk);

    List<TupleChunkFragmentEntity> fragmentEntities1 = generateFragmentsForChunk(mGf2nTupleChunk);

    fragmentStorageService.addUniqueConstraint();

    // fragmentStorageService.keep(fragmentEntities1.remove(0));
    fragmentStorageService.keepRound(fragmentEntities1);

    // heartbeatRepository.insertIntoHeartbeatTable(new String(RandomUtils.nextBytes(16)));

    fragmentStorageService.keep(fragmentEntities);
    // fragmentStorageService.keepRound(fragmentEntities);

    //    fragmentStorageService.keep(generateFragmentsForChunk(
    //            TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), UUID.randomUUID(),
    // RandomUtils.nextBytes(
    //            GFP.getElementSize() * TupleType.MULTIPLICATION_TRIPLE_GFP.getArity() * 50000 *
    // 2))));

    ArrayList<TupleChunkFragmentEntity> roundEntities =
        fragmentStorageService.retrieveAndReserveRoundFragments(
            50000, tupleType2, "3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    ArrayList<ReservationElement> reservationElements = mapToResElement(roundEntities);
    System.out.println(reservationElements);
  }

  protected ArrayList<ReservationElement> mapToResElement(
      ArrayList<TupleChunkFragmentEntity> frags) {
    return frags.stream()
        .map(
            t ->
                new ReservationElement(
                    t.getTupleChunkId(),
                    castorServiceProperties.getInitialFragmentSize(),
                    t.getStartIndex()))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  protected List<TupleChunkFragmentEntity> generateFragmentsForChunk(TupleChunk tupleChunk) {
    List<TupleChunkFragmentEntity> fragments = new ArrayList<>();
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
  void givenNoFragmentWithChunkIdInDb_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 0;

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, startIndex));
  }

  @Test
  void givenFragmentForChunkIdNotActivated_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 0;
    long actualStartIndex = 0;
    long actualLength = 42;

    fragmentRepository.save(
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, actualStartIndex, actualLength));

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  void givenFragmentForChunkIdIsReserved_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 0;
    long actualStartIndex = 0;
    long actualLength = 42;
    String actualReservationId = "already reserved";

    fragmentRepository.save(
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            actualStartIndex,
            actualLength,
            UNLOCKED,
            actualReservationId,
            true));

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  void
      givenNoFragmentForChunkIdContainingRequestedIndex_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 42;
    long actualStartIndex = 0;
    long actualEndIndex = 41;
    String actualReservationId = null;

    fragmentRepository.save(
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            actualStartIndex,
            actualEndIndex,
            UNLOCKED,
            actualReservationId,
            true));

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  void givenFragmentMatchingCriteria_whenFindAvailableFragment_thenReturnExpectedFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 42;
    long actualStartIndex = 41;
    long actualEndIndex = 43;
    String actualReservationId = null;

    TupleChunkFragmentEntity expectedFragment =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            actualStartIndex,
            actualEndIndex,
            UNLOCKED,
            actualReservationId,
            true);

    fragmentRepository.save(expectedFragment);

    assertEquals(
        Optional.of(expectedFragment),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  void givenMultipleFragmentsStored_whenFindAvailableFragment_thenReturnExpectedFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 42;

    TupleChunkFragmentEntity fragmentBefore =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, 0, requestedStartIndex - 1, UNLOCKED, null, true);
    TupleChunkFragmentEntity expectedFragment =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex,
            requestedStartIndex + 1,
            UNLOCKED,
            null,
            true);
    TupleChunkFragmentEntity fragmentAfter =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, requestedStartIndex + 1, Long.MAX_VALUE, UNLOCKED, null, true);

    fragmentRepository.save(fragmentBefore);
    fragmentRepository.save(expectedFragment);
    fragmentRepository.save(fragmentAfter);

    assertEquals(
        Optional.of(expectedFragment),
        fragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  void
      givenNoFragmentForRequestedTypeInDb_whenFindAvailableFragmentForType_thenReturnEmptyOptional() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  void
      givenNoUnlockedFragmentForRequestedTypeInDb_whenFindAvailableFragmentForType_thenReturnEmptyOptional() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;

    TupleChunkFragmentEntity lockedFragment =
        TupleChunkFragmentEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"), requestedTupleType, 0, 42);
    fragmentRepository.save(lockedFragment);

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  void
      givenNoAvailableFragmentForRequestedTypeInDb_whenFindAvailableFragmentForType_thenReturnEmptyOptional() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    String actualReservationId = "alreadyReserved";

    TupleChunkFragmentEntity reservedFragment =
        TupleChunkFragmentEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            requestedTupleType,
            0,
            42,
            UNLOCKED,
            actualReservationId,
            true);
    fragmentRepository.save(reservedFragment);

    assertEquals(
        Optional.empty(),
        fragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  void giveFragmentMatchesCriteria_whenFindAvailableFragmentForType_thenReturnFragment() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;

    TupleChunkFragmentEntity expectedFragment =
        TupleChunkFragmentEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            requestedTupleType,
            0,
            42,
            UNLOCKED,
            null,
            true);
    fragmentRepository.save(expectedFragment);

    assertEquals(
        Optional.of(expectedFragment),
        fragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  void
      giveMultipleFragmentsAvailable_whenFindAvailableFragmentForType_thenReturnFragmentWithLowestId() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;

    TupleChunkFragmentEntity expectedFragment =
        TupleChunkFragmentEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            requestedTupleType,
            0,
            42,
            UNLOCKED,
            null,
            true);
    TupleChunkFragmentEntity additionalFragment1 =
        TupleChunkFragmentEntity.of(
            UUID.fromString("3dc08ff2-5eed-49a9-979e-3a3ac0e4a2cf"),
            requestedTupleType,
            0,
            42,
            UNLOCKED,
            null,
            true);
    TupleChunkFragmentEntity additionalFragment2 =
        TupleChunkFragmentEntity.of(
            UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad"),
            requestedTupleType,
            0,
            42,
            UNLOCKED,
            null,
            true);

    fragmentRepository.save(additionalFragment1);
    fragmentRepository.save(expectedFragment);
    fragmentRepository.save(additionalFragment2);
    fragmentRepository.delete(additionalFragment1);

    assertEquals(
        Optional.of(expectedFragment),
        fragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  void givenNoConflictingFragments_whenCheckNoConflict_thenDoNothing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 42;
    long requestedEndIndex = 51;

    TupleChunkFragmentEntity fragmentBefore =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, 0, requestedStartIndex, UNLOCKED, null, true);
    TupleChunkFragmentEntity fragmentAfter =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, requestedEndIndex, Long.MAX_VALUE, UNLOCKED, null, true);
    fragmentRepository.save(fragmentBefore);
    fragmentRepository.save(fragmentAfter);

    try {
      fragmentStorageService.checkNoConflict(tupleChunkId, requestedStartIndex, requestedEndIndex);
    } catch (Exception e) {
      fail("Method not expected to throw exception: " + e.getMessage());
    }
  }

  @Test
  void givenConflictingFragment_whenCheckNoConflict_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 42;
    long requestedEndIndex = 51;

    TupleChunkFragmentEntity conflictingFragment =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex + 1,
            requestedEndIndex - 1,
            LOCKED,
            "alreadyReserved",
            true);
    fragmentRepository.save(conflictingFragment);

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () ->
                fragmentStorageService.checkNoConflict(
                    tupleChunkId, requestedStartIndex, requestedEndIndex));
    assertEquals(CONFLICT_EXCEPTION_MSG, actualCce.getMessage());
  }

  @Test
  void giveMultipleFragmentsInDb_whenCountAvailableTuples_thenReturnExpectedCount() {
    TupleType requestedType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    TupleChunkFragmentEntity fragmentOfDifferentType =
        TupleChunkFragmentEntity.of(
            UUID.randomUUID(), TupleType.INPUT_MASK_GFP, 0, Long.MAX_VALUE, UNLOCKED, null, true);
    TupleChunkFragmentEntity lockedFragment =
        TupleChunkFragmentEntity.of(
            UUID.randomUUID(), requestedType, 0, Long.MAX_VALUE, LOCKED, null, true);
    TupleChunkFragmentEntity reservedFragment =
        TupleChunkFragmentEntity.of(
            UUID.randomUUID(), requestedType, 0, Long.MAX_VALUE, UNLOCKED, "alreadyReserved", true);
    TupleChunkFragmentEntity oneFragment =
        TupleChunkFragmentEntity.of(UUID.randomUUID(), requestedType, 0, 12, UNLOCKED, null, true);
    TupleChunkFragmentEntity anotherFragment =
        TupleChunkFragmentEntity.of(
            UUID.randomUUID(), requestedType, 111, 141, UNLOCKED, null, true);

    fragmentRepository.save(fragmentOfDifferentType);
    fragmentRepository.save(lockedFragment);
    fragmentRepository.save(reservedFragment);
    fragmentRepository.save(oneFragment);
    fragmentRepository.save(anotherFragment);

    assertEquals(
        oneFragment.getEndIndex()
            - oneFragment.getStartIndex()
            + anotherFragment.getEndIndex()
            - anotherFragment.getStartIndex(),
        fragmentStorageService.getAvailableTuples(requestedType));
  }

  @Test
  void giveMultipleFragmentsInDb_whenActivateForChunk_thenUpdateAccordingly() {
    UUID requestedTupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    UUID differentChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");

    TupleChunkFragmentEntity fragmentForDifferentChunk =
        TupleChunkFragmentEntity.of(
            differentChunkId, TupleType.INPUT_MASK_GFP, 0, Long.MAX_VALUE, LOCKED, null, true);
    TupleChunkFragmentEntity oneFragment =
        TupleChunkFragmentEntity.of(
            requestedTupleChunkId, TupleType.MULTIPLICATION_TRIPLE_GFP, 0, 12, LOCKED, null, true);
    TupleChunkFragmentEntity anotherFragment =
        TupleChunkFragmentEntity.of(
            requestedTupleChunkId, TupleType.SQUARE_TUPLE_GF2N, 111, 141, LOCKED, null, true);

    fragmentForDifferentChunk = fragmentRepository.save(fragmentForDifferentChunk);
    oneFragment = fragmentRepository.save(oneFragment);
    anotherFragment = fragmentRepository.save(anotherFragment);

    fragmentStorageService.activateFragmentsForTupleChunk(requestedTupleChunkId);

    assertEquals(
        Arrays.asList(
            fragmentForDifferentChunk,
            oneFragment.setActivationStatus(UNLOCKED),
            anotherFragment.setActivationStatus(UNLOCKED)),
        fragmentRepository.findAll());
  }

  @Test
  void giveMultipleFragmentsInDb_whenDeleteByReservationId_thenDeleteAccordingly() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    String reservationId = "testReservation";

    TupleChunkFragmentEntity unreservedFragment =
        TupleChunkFragmentEntity.of(
            chunkId, TupleType.INPUT_MASK_GFP, 0, Long.MAX_VALUE, LOCKED, null, true);
    TupleChunkFragmentEntity oneFragment =
        TupleChunkFragmentEntity.of(
            chunkId, TupleType.MULTIPLICATION_TRIPLE_GFP, 0, 12, LOCKED, reservationId, true);
    TupleChunkFragmentEntity anotherFragment =
        TupleChunkFragmentEntity.of(
            chunkId, TupleType.SQUARE_TUPLE_GF2N, 111, 141, LOCKED, reservationId, true);

    unreservedFragment = fragmentRepository.save(unreservedFragment);
    fragmentRepository.save(oneFragment);
    fragmentRepository.save(anotherFragment);

    fragmentStorageService.deleteAllForReservationId(reservationId);

    assertEquals(1, fragmentRepository.count());
    assertEquals(singletonList(unreservedFragment), fragmentRepository.findAll());
  }

  @Test
  void givenNoFragmentAssociatedWithChunkId_whenCheckReferenced_thenReturnFalse() {
    UUID requestedTupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    UUID differentChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleChunkFragmentEntity fragmentForDifferentChunk =
        TupleChunkFragmentEntity.of(
            differentChunkId, TupleType.INPUT_MASK_GFP, 0, Long.MAX_VALUE, LOCKED, null, true);

    fragmentRepository.save(fragmentForDifferentChunk);

    assertFalse(fragmentStorageService.isChunkReferencedByFragments(requestedTupleChunkId));
  }

  @Test
  void givenAnyFragmentAssociatedWithChunkId_whenCheckReferenced_thenReturnTrue() {
    UUID requestedTupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleChunkFragmentEntity fragmentForDifferentChunk =
        TupleChunkFragmentEntity.of(
            requestedTupleChunkId,
            TupleType.INPUT_MASK_GFP,
            0,
            Long.MAX_VALUE,
            LOCKED,
            "reserved",
            true);

    fragmentRepository.save(fragmentForDifferentChunk);

    assertTrue(fragmentStorageService.isChunkReferencedByFragments(requestedTupleChunkId));
  }
}
