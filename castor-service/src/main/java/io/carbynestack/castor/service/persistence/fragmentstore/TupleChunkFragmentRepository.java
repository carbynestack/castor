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
import java.util.ArrayList;
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
import org.springframework.transaction.annotation.Isolation;
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
              + START_INDEX_FIELD
              + "<=:startIndex "
              + "AND "
              + END_INDEX_FIELD
              + ">:startIndex"
              + " ORDER BY "
              + START_INDEX_FIELD
              + " DESC")
  Optional<TupleChunkFragmentEntity> mockFindAvailableFragmentForTupleChunkContainingIndex(
      @Param("tupleChunkId") UUID tupleChunkId, @Param("startIndex") long startIndex);

  @Transactional(isolation = Isolation.SERIALIZABLE)
  @QueryHints({@QueryHint(name = "javax.persistence.lock.timeout", value = "3000")})
  @Query(
      value = "SELECT f FROM " + CLASS_NAME + " f " + "WHERE " + TUPLE_TYPE_FIELD + "=:tupleType ")
  ArrayList<TupleChunkFragmentEntity> findAvailableFragmentsForTupleType(
      @Param("tupleType") TupleType type);

  @Transactional
  @Modifying
  @Query(
      value =
          "CREATE INDEX IF NOT EXISTS "
              + TUPLECHUNK_STARTINDEX_INDEX_NAME
              + " ON "
              + TABLE_NAME
              + "("
              + TUPLE_CHUNK_ID_COLUMN
              + ", "
              + START_INDEX_COLUMN
              + ")",
      nativeQuery = true)
  void addIndexChunkIdAndStartIndex();

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
              //              + ACTIVATION_STATUS_FIELD
              //              + "=io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED "
              //              + "AND "
              + RESERVATION_ID_FIELD
              + " IS NULL "
              + "AND "
              + TUPLE_TYPE_FIELD
              + "=:tupleType")
  long getAvailableTuplesByType(@Param("tupleType") TupleType type);

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
  @Query(
      value =
          " UPDATE "
              + TABLE_NAME
              + " SET "
              + RESERVATION_ID_COLUMN
              + " = :reservationId"
              + " WHERE "
              + FRAGMENT_ID_COLUMN
              + " IN (SELECT "
              + FRAGMENT_ID_COLUMN
              + " FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_TYPE_COLUMN
              + " = :tupleType AND "
              + RESERVATION_ID_COLUMN
              + " is NULL AND "
              + IS_ROUND_COLUMN
              + " FOR UPDATE SKIP LOCKED LIMIT :amount) RETURNING *",
      nativeQuery = true)
  ArrayList<TupleChunkFragmentEntity> retrieveAndReserveRoundFragmentsByType(
      @Param("tupleType") String tupleType,
      @Param("amount") int amount,
      @Param("reservationId") String reservationId);

  @Transactional
  @Modifying
  @Query(
      value =
          "UPDATE "
              + TABLE_NAME
              + " SET "
              + RESERVATION_ID_COLUMN
              + " = :reservationId WHERE "
              + FRAGMENT_ID_COLUMN
              + " IN (SELECT "
              + FRAGMENT_ID_COLUMN
              + " FROM "
              + TABLE_NAME
              + " WHERE "
              + START_INDEX_COLUMN
              + " IN :indices AND "
              + IS_ROUND_COLUMN
              + " AND "
              + RESERVATION_ID_COLUMN
              + " is NULL AND "
              + TUPLE_CHUNK_ID_COLUMN
              + " = :tupleChunkId FOR UPDATE NOWAIT)",
      nativeQuery = true)
  int reserveRoundFragmentsByIndices(
      @Param("indices") ArrayList<Long> indices,
      @Param("reservationId") String reservationId,
      @Param("tupleChunkId") UUID tupleChunkId);

  @Transactional
  @Modifying
  @Query(
      value =
          "UPDATE "
              + TABLE_NAME
              + " SET "
              + RESERVATION_ID_COLUMN
              + " = :reservationId WHERE "
              + START_INDEX_COLUMN
              + " IN :indices AND "
              + IS_ROUND_COLUMN
              + " AND "
              + TUPLE_CHUNK_ID_COLUMN
              + " = :tupleChunkId",
      nativeQuery = true)
  int mockReserveRoundFragmentsByIndices(
      @Param("indices") ArrayList<Long> indices,
      @Param("reservationId") String reservationId,
      @Param("tupleChunkId") UUID tupleChunkId);

  @Transactional
  @Query(
      value =
          "SELECT * FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_TYPE_COLUMN
              + " = :tupleType AND "
              + RESERVATION_ID_COLUMN
              + " is NULL ORDER BY "
              + IS_ROUND_COLUMN
              + " FOR UPDATE SKIP LOCKED LIMIT 1",
      nativeQuery = true)
  Optional<TupleChunkFragmentEntity> retrieveSinglePartialFragment(
      @Param("tupleType") String tupleType);

  @Transactional
  @Query(
      value =
          " UPDATE "
              + TABLE_NAME
              + " SET "
              + RESERVATION_ID_COLUMN
              + " = :reservationId"
              + " WHERE "
              + FRAGMENT_ID_COLUMN
              + " IN (SELECT "
              + FRAGMENT_ID_COLUMN
              + " FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_TYPE_COLUMN
              + " = :tupleType AND "
              + IS_ROUND_COLUMN
              + " AND "
              + RESERVATION_ID_COLUMN
              + " is NULL FOR UPDATE SKIP LOCKED LIMIT :amount) RETURNING *",
      nativeQuery = true)
  ArrayList<TupleChunkFragmentEntity> test(
      @Param("tupleType") String tupleType,
      @Param("amount") int amount,
      @Param("reservationId") String reservationId);

  @Transactional
  @Query(
      value =
          "DELETE FROM "
              + TABLE_NAME
              + " WHERE "
              + FRAGMENT_ID_COLUMN
              + " IN (SELECT "
              + FRAGMENT_ID_COLUMN
              + " FROM "
              + TABLE_NAME
              + " WHERE "
              + RESERVATION_ID_COLUMN
              + " = :reservationId ORDER BY "
              + IS_ROUND_COLUMN
              + " FOR UPDATE NOWAIT) RETURNING *",
      nativeQuery = true)
  ArrayList<TupleChunkFragmentEntity> lockAndRetrieveReservedTuplesForConsumption(
      @Param("reservationId") String reservationId);

  @Transactional
  @Query(
      value =
          "DELETE FROM "
              + TABLE_NAME
              + " WHERE "
              + FRAGMENT_ID_COLUMN
              + " IN (SELECT "
              + FRAGMENT_ID_COLUMN
              + " FROM "
              + TABLE_NAME
              + " WHERE "
              + RESERVATION_ID_COLUMN
              + " = :reservationId FOR UPDATE NOWAIT)",
      nativeQuery = true)
  @Modifying
  int lockTuplesWithoutRetrievingForConsumption(@Param("reservationId") String reservationId);

  @Transactional
  @Query(
      value =
          "DELETE FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_CHUNK_ID_COLUMN
              + " = :tupleChunkId AND "
              + START_INDEX_COLUMN
              + " IN :startIndices RETURNING "
              + START_INDEX_COLUMN,
      nativeQuery = true)
  ArrayList<Integer> deleteByChunkAndStartIndex(
      @Param("tupleChunkId") UUID tupleChunkId,
      @Param("startIndices") ArrayList<Integer> startIndexes);

  @Transactional
  @Query(
      value =
          "SELECT * FROM "
              + VIEW_NAME
              + " WHERE "
              + POD_HASH_FIELD
              + " = :podHash ORDER BY "
              + FRAGMENT_ID_COLUMN,
      nativeQuery = true)
  ArrayList<TupleChunkFragmentEntity> getAssignedFragments(@Param("podHash") String podHash);

  @Transactional
  @Modifying
  @Query(
      value =
          "ALTER TABLE "
              + TABLE_NAME
              + " ADD CONSTRAINT no_conflict_unq UNIQUE("
              + TUPLE_CHUNK_ID_COLUMN
              + ", "
              + START_INDEX_COLUMN
              + ")",
      nativeQuery = true)
  void addUniqueConstraint();

  @Transactional
  void deleteAllByTupleChunkId(String tupleChunkId);

  @Transactional
  void deleteAllByReservationId(String reservationId);

  @Transactional
  boolean existsByTupleChunkId(UUID tupleChunkId);
}
