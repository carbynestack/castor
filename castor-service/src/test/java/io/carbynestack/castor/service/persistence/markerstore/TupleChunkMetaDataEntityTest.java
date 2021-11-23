/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.markerstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carbynestack.castor.common.entities.TupleType;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TupleChunkMetaDataEntityTest {

  @Test
  void givenIdIsNull_whenSetTupleChunkId_thenThrowIllegalArgumentException() {
    UUID nullId = null;
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long numberOfTuples = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkMetaDataEntity.of(nullId, tupleType, numberOfTuples));

    assertEquals(
        TupleChunkMetaDataEntity.ID_MUST_NOT_BE_NULL_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void givenNumberOfTuplesIsNegative_whenSetNumberOfTuples_thenThrowIllegalArgumentException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long invalidNumberOfTuples = -2;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkMetaDataEntity.of(chunkId, tupleType, invalidNumberOfTuples));

    assertEquals(
        TupleChunkMetaDataEntity.INVALID_NUMBER_OF_TUPLES_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void givenTupleTypeIsNull_whenSetTupleChunkId_thenThrowIllegalArgumentException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType nullTupleType = null;
    long numberOfTuples = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkMetaDataEntity.of(chunkId, nullTupleType, numberOfTuples));

    assertEquals(
        TupleChunkMetaDataEntity.TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void givenReservedMarkerIsNegative_whenSetReservedMarker_thenThrowIllegalArgumentException() {
    TupleChunkMetaDataEntity metaDataEntity =
        TupleChunkMetaDataEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            TupleType.MULTIPLICATION_TRIPLE_GFP,
            42);
    long invalidReservedMarker = -1;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> metaDataEntity.setReservedMarker(invalidReservedMarker));

    assertEquals(
        TupleChunkMetaDataEntity.MARKER_OUT_OF_RANGE_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void
      givenReservedMarkerIsLargerThanNumberOfTuples_whenSetReservedMarker_thenThrowIllegalArgumentException() {
    long numberOfTuples = 42;
    long invalidReservedMarker = numberOfTuples + 1;
    TupleChunkMetaDataEntity metaDataEntity =
        TupleChunkMetaDataEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            TupleType.MULTIPLICATION_TRIPLE_GFP,
            numberOfTuples);

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> metaDataEntity.setReservedMarker(invalidReservedMarker));

    assertEquals(
        TupleChunkMetaDataEntity.MARKER_OUT_OF_RANGE_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void givenConsumedMarkerIsNegative_whenSetReservedMarker_thenThrowIllegalArgumentException() {
    TupleChunkMetaDataEntity metaDataEntity =
        TupleChunkMetaDataEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            TupleType.MULTIPLICATION_TRIPLE_GFP,
            42);
    long invalidReservedMarker = -1;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> metaDataEntity.setConsumedMarker(invalidReservedMarker));

    assertEquals(
        TupleChunkMetaDataEntity.MARKER_OUT_OF_RANGE_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void
      givenConsumedMarkerIsLargerThanNumberOfTuples_whenSetReservedMarker_thenThrowIllegalArgumentException() {
    long numberOfTuples = 42;
    long invalidReservedMarker = numberOfTuples + 1;
    TupleChunkMetaDataEntity metaDataEntity =
        TupleChunkMetaDataEntity.of(
            UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"),
            TupleType.MULTIPLICATION_TRIPLE_GFP,
            numberOfTuples);

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> metaDataEntity.setConsumedMarker(invalidReservedMarker));

    assertEquals(
        TupleChunkMetaDataEntity.MARKER_OUT_OF_RANGE_EXCEPTION_MSG, actualIae.getMessage());
  }
}
