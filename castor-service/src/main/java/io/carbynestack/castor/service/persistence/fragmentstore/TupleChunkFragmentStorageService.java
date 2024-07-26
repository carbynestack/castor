/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import io.micrometer.core.annotation.Timed;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TupleChunkFragmentStorageService {
  public static final String CONFLICT_EXCEPTION_MSG =
      "At least one tuple described by the given sequence is already referenced by another"
          + " TupleChunkFragment.";
  public static final String NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG =
      "Not a single fragment associated with the given identifier.";

  private final TupleChunkFragmentRepository fragmentRepository;

  /**
   * Creates and stores an {@link TupleChunkFragmentEntity} object with the given information in the
   * database
   *
   * @param fragment The {@link TupleChunkFragmentEntity} to persist.
   * @return The stored / updated {@link TupleChunkFragmentEntity}.
   * @throws IllegalArgumentException In case the given fragment is null.
   * @throws CastorClientException If any of the tuples referenced by the given {@link
   *     TupleChunkFragmentEntity} is already covered by another {@link TupleChunkFragmentEntity}.
   */
  @Transactional
  public TupleChunkFragmentEntity keep(@NotNull TupleChunkFragmentEntity fragment)
      throws CastorClientException {
    checkNoConflict(fragment.getTupleChunkId(), fragment.getStartIndex(), fragment.getEndIndex());
    return fragmentRepository.save(fragment);
  }

  /**
   * Persists a List of {@link TupleChunkFragmentEntity}
   *
   * @param fragments List of {@link TupleChunkFragmentEntity}s to persist.
   * @throws IllegalArgumentException If the list or any of the given fragments is {@code null}.
   * @throws CastorClientException If any of the tuples referenced by the given {@link
   *     TupleChunkFragmentEntity} is already covered by another {@link TupleChunkFragmentEntity}.
   */
  @Transactional
  public void keep(@NotNull List<TupleChunkFragmentEntity> fragments) throws CastorClientException {
    for (TupleChunkFragmentEntity fragment : fragments) {
      checkNoConflict(fragment.getTupleChunkId(), fragment.getStartIndex(), fragment.getEndIndex());
    }
    fragmentRepository.saveAll(fragments);
  }

  /**
   * Gets the {@link TupleChunkFragmentEntity} for the {@link TupleChunk} with the given id that
   * meets the following criteria:
   *
   * <ul>
   *   <li>is available for consumption having {@link
   *       TupleChunkFragmentEntity#getActivationStatus()} set to {@link ActivationStatus#UNLOCKED}.
   *   <li>is not yet reserved to any other operation (having {@link
   *       TupleChunkFragmentEntity#getReservationId()} set to <code>null</code>.
   *   <li>contains the specified start index in the referenced tuple sequence.
   * </ul>
   *
   * @param tupleChunkId The id of the {@link TupleChunk} the {@link TupleChunkFragmentEntity}
   *     should reference.
   * @param startIndex The index of the tuple the {@link TupleChunkFragmentEntity} should reference.
   * @return Either an {@link Optional} containing the {@link TupleChunkFragmentEntity} that meets
   *     the described criteria or {@link Optional#empty()}.
   */
  @Transactional
  public Optional<TupleChunkFragmentEntity> findAvailableFragmentForChunkContainingIndex(
      UUID tupleChunkId, long startIndex) {
    return fragmentRepository.findAvailableFragmentForTupleChunkContainingIndex(
        tupleChunkId, startIndex);
  }

  /**
   * Gets a {@link TupleChunkFragmentEntity} that meets the following criteria:
   *
   * <ul>
   *   <li>is available for consumption having {@link
   *       TupleChunkFragmentEntity#getActivationStatus()} set to {@link ActivationStatus#UNLOCKED}.
   *   <li>is not yet reserved to any other operation (having {@link
   *       TupleChunkFragmentEntity#getReservationId()} set to <code>null</code>.
   *   <li>references a chunk for the requested {@link TupleType}.
   * </ul>
   *
   * @param tupleType The requested type of tuples.
   * @return Either an {@link Optional} containing the {@link TupleChunkFragmentEntity} that meets
   *     the described criteria or {@link Optional#empty()}.
   */
  @Transactional
  public Optional<TupleChunkFragmentEntity> findAvailableFragmentWithTupleType(
      TupleType tupleType) {
    return fragmentRepository
        .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
            tupleType, ActivationStatus.UNLOCKED);
  }

  /**
   * Splits a {@link TupleChunkFragmentEntity} at the given index by setting the {@link
   * TupleChunkFragmentEntity#getEndIndex()} to the given index and creating an additional {@link
   * TupleChunkFragmentEntity} for the remaining tuples.
   *
   * <p>The given fragment will remain untouched if the defined index is not <b>within</b> the
   * fragment's bounds.
   *
   * @param fragment the {@link TupleChunkFragmentEntity} to split.
   * @param index the index where to split the given fragment (<b>exclusive</b> to the returned,
   *     initial fragment)
   * @return the given fragment with its reduced size
   */
  @Transactional
  public TupleChunkFragmentEntity splitAt(TupleChunkFragmentEntity fragment, long index) {
    String oldFragmentState = null;
    if (log.isDebugEnabled()) {
      oldFragmentState = fragment.toString();
    }
    TupleChunkFragmentEntity nf =
        TupleChunkFragmentEntity.of(
            fragment.getTupleChunkId(),
            fragment.getTupleType(),
            index,
            fragment.getEndIndex(),
            fragment.getActivationStatus(),
            fragment.getReservationId());
    fragmentRepository.save(nf);
    fragment.setEndIndex(index);
    log.debug(
        "Fragment {} split at index {} into\n\tnew Fragment {}\n\tand updated own state to {}",
        oldFragmentState,
        index,
        nf,
        fragment);
    return fragmentRepository.save(fragment);
  }

  /**
   * Splits a {@link TupleChunkFragmentEntity} at the given index by setting the {@link
   * TupleChunkFragmentEntity#getStartIndex()} to the given index and creating an additional {@link
   * TupleChunkFragmentEntity} for the remaining tuples.
   *
   * <p>The given fragment will remain untouched if the defined index is not <b>within</b> the
   * fragment's bounds.
   *
   * @param fragment the {@link TupleChunkFragmentEntity} to split.
   * @param index the index where to split the given fragment (<b>inclusive</b> to the newly created
   *     fragment)
   * @return the newly created fragment beginning at the given index or the untouched fragment if
   *     the split index is not within the fragment's range
   */
  @Transactional
  public TupleChunkFragmentEntity splitBefore(TupleChunkFragmentEntity fragment, long index) {
    if (index <= fragment.getStartIndex() || index >= fragment.getEndIndex()) {
      return fragment;
    }
    String oldFragmentState = null;
    if (log.isDebugEnabled()) {
      oldFragmentState = fragment.toString();
    }
    TupleChunkFragmentEntity nf =
        TupleChunkFragmentEntity.of(
            fragment.getTupleChunkId(),
            fragment.getTupleType(),
            index,
            fragment.getEndIndex(),
            fragment.getActivationStatus(),
            fragment.getReservationId());
    fragment.setEndIndex(index);
    fragmentRepository.save(fragment);
    log.debug(
        "Fragment {} split before index {} into\n\tnew Fragment {}\n\tand updated own state to {}",
        oldFragmentState,
        index,
        nf,
        fragment);
    return fragmentRepository.save(nf);
  }

  /**
   * Verifies that a consecutive section of tuple(s) within a {@link TupleChunk} is not yet
   * described by a {@link TupleChunkFragmentEntity}.
   *
   * @param chunkId The unique identifier of the referenced {@link TupleChunk}
   * @param startIndex The beginning of the tuple section to check
   * @param endIndex The end of the tuple section to check (<b>exclusive</b>)
   * @throws CastorClientException If at least one tuple in the given section is already referenced
   *     by a {@link TupleChunkFragmentEntity}
   */
  @Transactional(readOnly = true)
  public void checkNoConflict(UUID chunkId, long startIndex, long endIndex) {
    if (fragmentRepository
        .findFirstFragmentContainingAnyTupleOfSequence(chunkId, startIndex, endIndex)
        .isPresent()) {
      throw new CastorClientException(CONFLICT_EXCEPTION_MSG);
    }
  }

  /**
   * Returns the number of available tuples of a given {@link TupleType}.
   *
   * <p>Tuples referenced by {@link TupleChunkFragmentEntity fragments} that are either {@link
   * ActivationStatus#LOCKED} or reserved (see {@link TupleChunkFragmentEntity#getReservationId()})
   * are not counted.
   *
   * @param type T{@link TupleType} of interest.
   * @return the number of available tuples for the given type.
   */
  public long getAvailableTuples(TupleType type) {
    try {
      return fragmentRepository.getAvailableTupleByType(type);
    } catch (Exception e) {
      log.debug(
          String.format(
              "FragmentRepository threw exception when requesting number of available %s.", type),
          e);
      return 0;
    }
  }

  /**
   * Activates all {@link TupleChunkFragmentEntity fragments} associated with the given tuple chunk
   * id.
   *
   * @param chunkId the unique identifier of the tuple chunk whose {@link TupleChunkFragmentEntity
   *     fragments} are to be activated.
   * @throws CastorServiceException if not a single {@link TupleChunkFragmentEntity} was associated
   *     with the given tuple chunk id
   */
  public void activateFragmentsForTupleChunk(UUID chunkId) {
    if (fragmentRepository.unlockAllForTupleChunk(chunkId) <= 0) {
      throw new CastorServiceException(NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG);
    }
  }

  /**
   * Removes all {@link TupleChunkFragmentEntity fragments} associated with the given reservation
   * id.
   *
   * @param reservationId the unique identifier of the reservation.
   */
  public void deleteAllForReservationId(String reservationId) {
    fragmentRepository.deleteAllByReservationId(reservationId);
  }

  /**
   * Indicates whether any {@link TupleChunkFragmentEntity fragment} is associated with the given
   * tuple chunk id.
   *
   * @param tupleChunkId the unique identifier of the tuple chunk of interest.
   * @return <code>true</code> if any {@link TupleChunkFragmentEntity fragment} is associated with
   *     the given chunk id, <code>false</code> if not.
   */
  @Transactional(readOnly = true)
  public boolean isChunkReferencedByFragments(UUID tupleChunkId) {
    return fragmentRepository.existsByTupleChunkId(tupleChunkId);
  }

  @Transactional
  public void update(TupleChunkFragmentEntity fragment) {
    fragmentRepository.save(fragment);
  }
}
