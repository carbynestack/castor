/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleType;
import java.util.UUID;
import org.junit.Test;

public class TupleChunkFragmentEntityTest {

  @Test
  public void givenTupleChunkIdIsNull_whenCreateWithFactoryMethod_thenThrowException() {
    UUID invalidChunkId = null;
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkFragmentEntity.of(invalidChunkId, tupleType, startIndex, endIndex));

    assertEquals(
        TupleChunkFragmentEntity.ID_MUST_NOT_BE_NULL_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  public void givenTupleTypeIsNull_whenCreateWithFactoryMethod_thenThrowException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType invalidTupleType = null;
    long startIndex = 0;
    long length = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkFragmentEntity.of(chunkId, invalidTupleType, startIndex, length));

    assertEquals(
        TupleChunkFragmentEntity.TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  public void givenStartIndexIsNegative_whenCreateWithFactoryMethod_thenThrowException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long invalidStartIndex = -2;
    long endIndex = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkFragmentEntity.of(chunkId, tupleType, invalidStartIndex, endIndex));

    assertEquals(
        String.format(
            TupleChunkFragmentEntity.INVALID_START_INDEX_EXCEPTION_FORMAT, invalidStartIndex),
        actualIae.getMessage());
  }

  @Test
  public void givenInvalidLength_whenCreateWithFactoryMethod_thenThrowException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long invalidEndIndex = -1;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> TupleChunkFragmentEntity.of(chunkId, tupleType, startIndex, invalidEndIndex));

    assertEquals(
        String.format(
            TupleChunkFragmentEntity.ILLEGAL_LAST_INDEX_EXCEPTION_FORMAT, invalidEndIndex),
        actualIae.getMessage());
  }

  @Test
  public void givenValidConfiguration_whenCreateWithFactoryMethod_thenReturnExpectedFragment() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long startIndex = 0;
    long endIndex = 42;
    ActivationStatus activationStatus = ActivationStatus.UNLOCKED;
    String reservationId = "123_multiplicationGfP";

    TupleChunkFragmentEntity fragment =
        TupleChunkFragmentEntity.of(
            chunkId, tupleType, startIndex, endIndex, activationStatus, reservationId);

    assertEquals(chunkId, fragment.getTupleChunkId());
    assertEquals(tupleType, fragment.getTupleType());
    assertEquals(startIndex, fragment.getStartIndex());
    assertEquals(endIndex, fragment.getEndIndex());
    assertEquals(activationStatus, fragment.getActivationStatus());
    assertEquals(reservationId, fragment.getReservationId());
  }

  @Test
  public void givenInvalidIndex_whenSetEndIndex_thenThrowException() {
    long invalidEndIndex = -1;

    TupleChunkFragmentEntity fragmentMock = new TupleChunkFragmentEntity();

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class, () -> fragmentMock.setEndIndex(invalidEndIndex));

    assertEquals(
        String.format(
            TupleChunkFragmentEntity.ILLEGAL_LAST_INDEX_EXCEPTION_FORMAT, invalidEndIndex),
        actualIae.getMessage());
  }
}
