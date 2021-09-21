/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.persistence.markerstore;

import io.carbynestack.castor.common.entities.TupleType;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TupleChunkMetadataRepository
    extends CrudRepository<TupleChunkMetaDataEntity, UUID> {

  @Transactional(readOnly = true)
  @Query(
      "SELECT tc "
          + "FROM "
          + TupleChunkMetaDataEntity.CLASS_NAME
          + " tc "
          + "WHERE tc."
          + TupleChunkMetaDataEntity.TUPLE_TYPE_FIELD
          + "=:tupleType "
          + " AND tc."
          + TupleChunkMetaDataEntity.STATUS_FIELD
          + "=io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED")
  List<TupleChunkMetaDataEntity> findAllUnlockedByTupleType(
      @Param("tupleType") TupleType tupleType);

  @Transactional(readOnly = true)
  @Query(
      "SELECT SUM("
          + TupleChunkMetaDataEntity.NUMBER_OF_TUPLES_FIELD
          + "-"
          + TupleChunkMetaDataEntity.RESERVED_MARKER_COLUMN
          + ") "
          + "FROM "
          + TupleChunkMetaDataEntity.CLASS_NAME
          + " "
          + "WHERE "
          + TupleChunkMetaDataEntity.TUPLE_TYPE_FIELD
          + "=:tupleType"
          + " AND "
          + TupleChunkMetaDataEntity.STATUS_FIELD
          + "=io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED")
  Long getAvailableTuplesByTupleType(@Param("tupleType") TupleType tupleType);
}
