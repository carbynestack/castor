/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.markerstore;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TupleChunkMetaDataStorageService {
  public static final String CONFLICT_FOR_ID_EXCEPTION_MSG =
      "Metadata for chunk with ID #%s does already exist.";
  public static final String NO_METADATA_FOR_OD_EXCEPTION_MSG =
      "No metadata found for TupleChunk #%s in database.";
  private final TupleChunkMetadataRepository tupleChunkMetadataRepository;

  /**
   * Creates and stores an {@link TupleChunkMetaDataEntity} object with the given information in the
   * database
   *
   * @param tupleChunkId
   * @param tupleType
   * @param numberOfTuples
   * @return
   * @throws CastorClientException in case there is already a {@link TupleChunkMetaDataEntity}
   *     object for the given ID in the database.
   */
  @Transactional
  public TupleChunkMetaDataEntity keepTupleChunkData(
      UUID tupleChunkId, TupleType tupleType, long numberOfTuples) throws CastorClientException {
    if (tupleChunkMetadataRepository.existsById(tupleChunkId)) {
      throw new CastorClientException(String.format(CONFLICT_FOR_ID_EXCEPTION_MSG, tupleChunkId));
    }

    TupleChunkMetaDataEntity metaData =
        TupleChunkMetaDataEntity.of(tupleChunkId, tupleType, numberOfTuples);
    return tupleChunkMetadataRepository.save(metaData);
  }

  /**
   * Retrieves the {@link TupleChunkMetaDataEntity} for the {@link TupleChunk} with the given id.
   *
   * @param tupleChunkId
   * @return The {@link TupleChunkMetaDataEntity} object for the {@link TupleChunk} with the given
   *     Id. Returns {@code null} if there is no {@link TupleChunkMetaDataEntity} stored in the
   *     database.
   */
  @Transactional(readOnly = true)
  public TupleChunkMetaDataEntity getTupleChunkData(UUID tupleChunkId) {
    return tupleChunkMetadataRepository.findById(tupleChunkId).orElse(null);
  }

  /**
   * Adds a given amount of tuples to the {@link TupleChunkMetaDataEntity#getReservedMarker()} of
   * the {@link TupleChunkMetaDataEntity} with the given {@link TupleChunk} Id.
   *
   * <p>This method is called as nester transaction ({@link Propagation#NESTED})
   *
   * @param tupleChunkId
   * @param reservedTuples
   * @return
   * @throws CastorClientException if metadata cannot be retrieved
   */
  @Transactional(propagation = Propagation.REQUIRED)
  public void updateReservationForTupleChunkData(UUID tupleChunkId, long reservedTuples) {
    TupleChunkMetaDataEntity metaData = getMetadataById(tupleChunkId);
    long newMarker = metaData.getReservedMarker() + reservedTuples;
    metaData.setReservedMarker(newMarker);
    tupleChunkMetadataRepository.save(metaData);
  }

  /**
   * Adds a given amount of tuples to the {@link TupleChunkMetaDataEntity#getConsumedMarker()} of
   * the {@link TupleChunkMetaDataEntity} with the given {@link TupleChunk} Id.
   *
   * <p>This method is called as nester transaction ({@link Propagation#NESTED})
   *
   * @param tupleChunkId
   * @param consumedTuples
   * @return
   * @throws CastorClientException if metadata cannot be retrieved
   */
  @Transactional(propagation = Propagation.REQUIRED)
  public TupleChunkMetaDataEntity updateConsumptionForTupleChunkData(
      UUID tupleChunkId, long consumedTuples) {
    TupleChunkMetaDataEntity metaData = getMetadataById(tupleChunkId);
    long newMarker = metaData.getConsumedMarker() + consumedTuples;
    metaData.setConsumedMarker(newMarker);
    tupleChunkMetadataRepository.save(metaData);
    return metaData;
  }

  /**
   * 'Activates' a tuple chunk by setting its {@link TupleChunkMetaDataEntity#getStatus()} to {@link
   * ActivationStatus#UNLOCKED}.
   *
   * @param tupleChunkId
   * @return the updated {@link TupleChunkMetaDataEntity} object.
   * @throws CastorClientException If there is no {@link TupleChunkMetaDataEntity} for a {@link
   *     TupleChunk} with the given ID.
   */
  @Transactional
  public TupleChunkMetaDataEntity activateTupleChunk(UUID tupleChunkId)
      throws CastorClientException {
    TupleChunkMetaDataEntity metaData = getMetadataById(tupleChunkId);
    metaData.setStatus(ActivationStatus.UNLOCKED);
    return tupleChunkMetadataRepository.save(metaData);
  }

  /**
   * Deletes the {@link TupleChunkMetaDataEntity} object for the {@link TupleChunk} with the given
   * id.
   *
   * @param tupleChunkId
   */
  @Transactional
  public void forgetTupleChunkData(UUID tupleChunkId) {
    tupleChunkMetadataRepository.deleteById(tupleChunkId);
  }

  /**
   * Returns a list of {@link TupleChunkMetaDataEntity} storing the given {@link TupleType}.
   *
   * @param tupleType
   * @return
   */
  @Transactional(readOnly = true)
  public List<TupleChunkMetaDataEntity> getTupleChunkData(TupleType tupleType) {
    return tupleChunkMetadataRepository.findAllUnlockedByTupleType(tupleType);
  }

  /**
   * Returns the overall number of available tuples for a given type.
   *
   * <p>This method considers unlocked {@link TupleChunk}s only
   *
   * @param tupleType
   * @return
   */
  @Transactional(readOnly = true)
  public long getAvailableTuples(TupleType tupleType) {
    try {
      return tupleChunkMetadataRepository.getAvailableTuplesByTupleType(tupleType);
    } catch (NullPointerException npe) {
      // @Query operation with SUM throws a nullpointer exception if there are no entries in the
      // database
      // matching the given criteria.
      return 0L;
    }
  }

  private TupleChunkMetaDataEntity getMetadataById(UUID tupleChunkId) {
    return tupleChunkMetadataRepository
        .findById(tupleChunkId)
        .orElseThrow(
            () ->
                new CastorClientException(
                    String.format(NO_METADATA_FOR_OD_EXCEPTION_MSG, tupleChunkId)));
  }
}
