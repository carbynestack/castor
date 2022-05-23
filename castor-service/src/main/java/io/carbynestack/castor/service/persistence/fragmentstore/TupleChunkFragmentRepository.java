/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleType;
import java.util.Optional;
import java.util.UUID;
import javax.persistence.LockModeType;
import javax.persistence.QueryHint;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TupleChunkFragmentRepository
    extends CrudRepository<TupleChunkFragmentEntity, Long> {
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints({@QueryHint(name = "javax.persistence.lock.timeout", value = "3000")})
  Optional<TupleChunkFragmentEntity>
      findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
          TupleType tupleType, ActivationStatus activationStatus);

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints({@QueryHint(name = "javax.persistence.lock.timeout", value = "3000")})
  @Query(
      value =
          "SELECT f FROM "
              + CLASS_NAME
              + " f "
              + "WHERE "
              + TUPLE_CHUNK_ID_FIELD
              + "=:tupleChunkId "
              + "AND "
              + ACTIVATION_STATUS_FIELD
              + "='UNLOCKED' "
              + "AND "
              + RESERVATION_ID_FIELD
              + " IS NULL "
              + "AND "
              + START_INDEX_FIELD
              + "<=:startIndex "
              + "AND "
              + END_INDEX_FIELD
              + ">:startIndex"
              + " ORDER BY "
              + START_INDEX_FIELD
              + " DESC")
  Optional<TupleChunkFragmentEntity> findAvailableFragmentForTupleChunkContainingIndex(
      @Param("tupleChunkId") UUID tupleChunkId, @Param("startIndex") long startIndex);

  @Transactional(readOnly = true)
  @Query(
      value =
          "SELECT f FROM "
              + CLASS_NAME
              + " f "
              + "WHERE "
              + TUPLE_CHUNK_ID_FIELD
              + "=:tupleChunkId "
              + "AND "
              + START_INDEX_FIELD
              + "<:endIndex "
              + "AND "
              + END_INDEX_FIELD
              + " >:startIndex")
  Optional<TupleChunkFragmentEntity> findFirstFragmentContainingAnyTupleOfSequence(
      @Param("tupleChunkId") UUID tupleChunkId,
      @Param("startIndex") long startIndex,
      @Param("endIndex") long endIndex);

  @Transactional(readOnly = true)
  @Query(
      value =
          "SELECT SUM ("
              + END_INDEX_FIELD
              + " - "
              + START_INDEX_FIELD
              + ")"
              + " FROM "
              + CLASS_NAME
              + " WHERE "
              + ACTIVATION_STATUS_FIELD
              + "=io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED "
              + "AND "
              + RESERVATION_ID_FIELD
              + " IS NULL "
              + "AND "
              + TUPLE_TYPE_FIELD
              + "=:tupleType")
  long getAvailableTupleByType(@Param("tupleType") TupleType type);

  @Transactional
  @Modifying
  @Query(
      value =
          "UPDATE "
              + TABLE_NAME
              + " SET "
              + ACTIVATION_STATUS_COLUMN
              + "='UNLOCKED' WHERE "
              + TUPLE_CHUNK_ID_COLUMN
              + " = :tupleChunkId",
      nativeQuery = true)
  int unlockAllForTupleChunk(@Param("tupleChunkId") UUID tupleChunkId);

  @Transactional
  void deleteAllByReservationId(String reservationId);

  @Transactional
  boolean existsByTupleChunkId(UUID tupleChunkId);
}
