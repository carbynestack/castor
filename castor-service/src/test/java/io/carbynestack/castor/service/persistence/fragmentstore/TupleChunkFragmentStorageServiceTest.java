/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService.CONFLICT_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService.NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.aop.AopInvocationException;

@RunWith(MockitoJUnitRunner.class)
public class TupleChunkFragmentStorageServiceTest {
  @Mock private TupleChunkFragmentRepository tupleChunkFragmentRepositoryMock;

  @InjectMocks private TupleChunkFragmentStorageService tupleChunkFragmentStorageService;

  @Test
  public void givenNoConflictingFragments_whenKeep_thenPersist() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 12;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, endIndex);

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, startIndex, endIndex))
        .thenReturn(Optional.empty());

    tupleChunkFragmentStorageService.keep(fragmentEntity);

    verify(tupleChunkFragmentRepositoryMock, times(1)).save(fragmentEntity);
  }

  @Test
  public void givenNoConflictingFragments_whenKeepList_thenPersist() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    int fragmentSize = 12;
    List<TupleChunkFragmentEntity> fragments =
        Arrays.asList(
            TupleChunkFragmentEntity.of(tupleChunkId, tupleType, 0, fragmentSize),
            TupleChunkFragmentEntity.of(tupleChunkId, tupleType, fragmentSize, fragmentSize * 2));

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            any(UUID.class), anyLong(), anyLong()))
        .thenReturn(Optional.empty());

    tupleChunkFragmentStorageService.keep(fragments);

    verify(tupleChunkFragmentRepositoryMock, times(1)).saveAll(fragments);
  }

  @Test
  public void givenConflictingFragment_whenKeepList_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    int fragmentSize = 12;
    TupleChunkFragmentEntity fragment1 =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, 0, fragmentSize);
    TupleChunkFragmentEntity conflictingFragment =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, fragmentSize, fragmentSize * 2);
    List<TupleChunkFragmentEntity> fragments = Arrays.asList(fragment1, conflictingFragment);

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, fragment1.getStartIndex(), fragment1.getEndIndex()))
        .thenReturn(Optional.empty());
    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, conflictingFragment.getStartIndex(), conflictingFragment.getEndIndex()))
        .thenReturn(Optional.of(conflictingFragment));

    CastorClientException actualCCE =
        assertThrows(
            CastorClientException.class, () -> tupleChunkFragmentStorageService.keep(fragments));

    assertEquals(CONFLICT_EXCEPTION_MSG, actualCCE.getMessage());
    verify(tupleChunkFragmentRepositoryMock, never()).saveAll(anyList());
  }

  @Test
  public void
      givenNoFragmentInDbMatchingCriteria_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 0;
    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, startIndex))
        .thenReturn(Optional.empty());

    assertEquals(
        Optional.empty(),
        tupleChunkFragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, startIndex));
  }

  @Test
  public void giveFragmentMatchesCriteria_whenFindAvailableFragment_thenReturnFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    TupleChunkFragmentEntity actualFragmentMock = new TupleChunkFragmentEntity();

    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.of(actualFragmentMock));

    assertEquals(
        Optional.of(actualFragmentMock),
        tupleChunkFragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  public void
      givenNoFragmentInDbMatchingCriteria_whenFindAvailableFragmentForType_thenReturnEmptyOptional() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    when(tupleChunkFragmentRepositoryMock
            .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
                requestedTupleType, ActivationStatus.UNLOCKED))
        .thenReturn(Optional.empty());

    assertEquals(
        Optional.empty(),
        tupleChunkFragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  public void giveFragmentMatchesCriteria_whenFindAvailableFragmentForType_thenReturnFragment() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    TupleChunkFragmentEntity actualFragmentMock = mock(TupleChunkFragmentEntity.class);

    when(tupleChunkFragmentRepositoryMock
            .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
                requestedTupleType, ActivationStatus.UNLOCKED))
        .thenReturn(Optional.of(actualFragmentMock));

    assertEquals(
        Optional.of(actualFragmentMock),
        tupleChunkFragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  public void givenSuccessfulRequest_whenSplitAt_thenSplitAccordinglyAndReturnAlteredFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 12;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, endIndex);
    long splitIndex = 7;
    TupleChunkFragmentEntity expectedAlteredFragment =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, splitIndex);
    TupleChunkFragmentEntity expectedNewFragment =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, splitIndex, endIndex);

    when(tupleChunkFragmentRepositoryMock.save(any()))
        .thenAnswer(
            (Answer<TupleChunkFragmentEntity>) invocationOnMock -> invocationOnMock.getArgument(0));

    assertEquals(
        expectedAlteredFragment,
        tupleChunkFragmentStorageService.splitAt(fragmentEntity, splitIndex));

    verify(tupleChunkFragmentRepositoryMock, times(1)).save(expectedAlteredFragment);
    verify(tupleChunkFragmentRepositoryMock, times(1)).save(expectedNewFragment);
    verifyNoMoreInteractions(tupleChunkFragmentRepositoryMock);
  }

  @Test
  public void givenSplitIndexOutOfRange_whenSplitBefore_thenDoNothing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 12;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, endIndex);
    long illegalSplitIndex = endIndex;

    assertEquals(
        fragmentEntity,
        tupleChunkFragmentStorageService.splitBefore(fragmentEntity, illegalSplitIndex));

    verify(tupleChunkFragmentRepositoryMock, never()).save(any());
  }

  @Test
  public void givenSuccessfulRequest_whenSplitBefore_thenSplitAccordinglyAndReturnNewFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 12;
    TupleChunkFragmentEntity fragmentEntity =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, endIndex);
    long splitIndex = 7;
    TupleChunkFragmentEntity expectedNewFragment =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, splitIndex, endIndex);
    TupleChunkFragmentEntity expectedAlteredFragment =
        TupleChunkFragmentEntity.of(tupleChunkId, tupleType, startIndex, splitIndex);

    when(tupleChunkFragmentRepositoryMock.save(any()))
        .thenAnswer(
            (Answer<TupleChunkFragmentEntity>) invocationOnMock -> invocationOnMock.getArgument(0));

    assertEquals(
        expectedNewFragment,
        tupleChunkFragmentStorageService.splitBefore(fragmentEntity, splitIndex));

    verify(tupleChunkFragmentRepositoryMock, times(1)).save(expectedAlteredFragment);
    verify(tupleChunkFragmentRepositoryMock, times(1)).save(expectedNewFragment);
    verifyNoMoreInteractions(tupleChunkFragmentRepositoryMock);
  }

  @Test
  public void givenConflictingFragment_whenCheckNoConflict_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 42;
    long endIndex = 44;
    Optional<TupleChunkFragmentEntity> nonEmptyOptional =
        Optional.of(mock(TupleChunkFragmentEntity.class));

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, startIndex, endIndex))
        .thenReturn(nonEmptyOptional);

    CastorClientException actualCCE =
        assertThrows(
            CastorClientException.class,
            () ->
                tupleChunkFragmentStorageService.checkNoConflict(
                    tupleChunkId, startIndex, endIndex));
    assertEquals(CONFLICT_EXCEPTION_MSG, actualCCE.getMessage());
  }

  @Test
  public void givenNoConflictingFragment_whenCheckNoConflict_thenDoNothing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 42;
    long endIndex = 44;

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, startIndex, endIndex))
        .thenReturn(Optional.empty());

    try {
      tupleChunkFragmentStorageService.checkNoConflict(tupleChunkId, startIndex, endIndex);
    } catch (Exception e) {
      fail("Method not expected to throw exception");
    }
  }

  @Test
  public void givenRepositoryThrowsException_whenGetAvailableTuples_thenReturnZero() {
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    AopInvocationException expectedException = new AopInvocationException("expected");

    when(tupleChunkFragmentRepositoryMock.getAvailableTupleByType(tupleType))
        .thenThrow(expectedException);

    assertEquals(0, tupleChunkFragmentStorageService.getAvailableTuples(tupleType));
  }

  @Test
  public void givenSuccessfulRequest_whenGetAvailableTuples_thenReturnExpectedResult() {
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long availableTuples = 42;

    when(tupleChunkFragmentRepositoryMock.getAvailableTupleByType(tupleType))
        .thenReturn(availableTuples);

    assertEquals(availableTuples, tupleChunkFragmentStorageService.getAvailableTuples(tupleType));
  }

  @Test
  public void
      givenNotASingleFragmentActivated_whenActivateFragmentsForTupleChunk_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    int numberOfActivatedFragments = 0;

    when(tupleChunkFragmentRepositoryMock.unlockAllForTupleChunk(tupleChunkId))
        .thenReturn(numberOfActivatedFragments);

    CastorServiceException actualCSE =
        assertThrows(
            CastorServiceException.class,
            () -> tupleChunkFragmentStorageService.activateFragmentsForTupleChunk(tupleChunkId));

    assertEquals(NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG, actualCSE.getMessage());
  }

  @Test
  public void givenSuccessfulRequest_whenActivateFragmentsForTupleChunk_thenDoNothing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    int numberOfActivatedFragments = 2;

    when(tupleChunkFragmentRepositoryMock.unlockAllForTupleChunk(tupleChunkId))
        .thenReturn(numberOfActivatedFragments);

    try {
      tupleChunkFragmentStorageService.activateFragmentsForTupleChunk(tupleChunkId);
    } catch (Exception e) {
      fail("Method not expected to throw exception");
    }
  }

  @Test
  public void givenSuccessfulRequest_whenDeleteAllForReservationId_thenDoNothing() {
    String reservationId = "reservationId";

    tupleChunkFragmentStorageService.deleteAllForReservationId(reservationId);

    verify(tupleChunkFragmentRepositoryMock, times(1)).deleteAllByReservationId(reservationId);
  }

  @Test
  public void
      givenFragmentWithGivenTupleChunkIdExists_whenRequestIsReferencedState_thenReturnTrue() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    boolean hasAtLeastOneFragmentWithGivenChunkId = true;

    when(tupleChunkFragmentRepositoryMock.existsByTupleChunkId(tupleChunkId))
        .thenReturn(hasAtLeastOneFragmentWithGivenChunkId);

    assertEquals(
        hasAtLeastOneFragmentWithGivenChunkId,
        tupleChunkFragmentStorageService.isChunkReferencedByFragments(tupleChunkId));
  }

  @Test
  public void givenSuccessfulRequest_whenDUpdateFragment_thenPersistFragment() {
    TupleChunkFragmentEntity fragment = mock(TupleChunkFragmentEntity.class);

    tupleChunkFragmentStorageService.update(fragment);

    verify(tupleChunkFragmentRepositoryMock, times(1)).save(fragment);
  }
}
