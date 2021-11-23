/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static io.carbynestack.castor.common.entities.ArrayBackedTuple.*;
import static io.carbynestack.castor.common.entities.Field.GFP;
import static org.junit.jupiter.api.Assertions.*;

import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

class ArrayBackedTupleTest {

  @Test
  void givenValidStream_whenCreateFromStream_thenReturnExpectedTuple() throws IOException {
    byte[] expectedTupleValueData = RandomUtils.nextBytes(GFP.getElementSize());
    byte[] expectedTupleMacData = RandomUtils.nextBytes(GFP.getElementSize());
    Share expectedShare = Share.of(expectedTupleValueData, expectedTupleMacData);
    Bit<Field.Gfp> actualBitTuple =
        new Bit<>(
            GFP,
            new ByteArrayInputStream(
                ArrayUtils.addAll(expectedTupleValueData, expectedTupleMacData)));
    assertEquals(1, actualBitTuple.getShares().length);
    assertEquals(expectedShare, actualBitTuple.getShare(0));
  }

  @Test
  void givenStreamOfInvalidLength_whenCreateFromStream_thenReturnExpectedTuple() {
    byte[] tupleValueData = RandomUtils.nextBytes(GFP.getElementSize());
    byte[] invalidTupleMacData = RandomUtils.nextBytes(GFP.getElementSize() - 1);
    IOException actualIoe =
        assertThrows(
            IOException.class,
            () ->
                new Bit<>(
                    GFP,
                    new ByteArrayInputStream(
                        ArrayUtils.addAll(tupleValueData, invalidTupleMacData))));
    assertEquals(NO_MORE_TUPLE_DATA_AVAILABLE_EXCEPTION_MSG, actualIoe.getMessage());
  }

  @Test
  void givenValidNumberOfShares_whenCreateNewTuple_thenReturnExpectedTuple() {
    byte[] expectedTupleValueData = RandomUtils.nextBytes(GFP.getElementSize());
    byte[] expectedTupleMacData = RandomUtils.nextBytes(GFP.getElementSize());
    Share expectedShare = Share.of(expectedTupleValueData, expectedTupleMacData);
    Bit<Field.Gfp> actualBitTuple = new Bit<>(GFP, expectedShare);
    assertArrayEquals(new Share[] {expectedShare}, actualBitTuple.getShares());
  }

  @Test
  void givenInvalidNumberOfShares_whenCreateNewTuple_thenThrowIllegalArgumentException() {
    IllegalArgumentException actualIae =
        assertThrows(IllegalArgumentException.class, () -> new Bit<>(GFP, new Share[0]));
    assertEquals(
        String.format(
            INVALID_NUMBER_OF_SHARES_EXCEPTION_MSG,
            Bit.class.getSimpleName(),
            TupleType.BIT_GFP.getArity()),
        actualIae.getMessage());
  }

  @Test
  void
      givenRequestedShareIndexIsOutOfBounds_whenRetrievingIndividualShare_thenThrowCastorClientException()
          throws IOException {
    int invalidIndex = TupleType.BIT_GFP.getArity() + 1;
    byte[] expectedTupleValueData = RandomUtils.nextBytes(GFP.getElementSize());
    byte[] expectedTupleMacData = RandomUtils.nextBytes(GFP.getElementSize());
    Bit<Field.Gfp> actualBitTuple =
        new Bit<>(
            GFP,
            new ByteArrayInputStream(
                ArrayUtils.addAll(expectedTupleValueData, expectedTupleMacData)));
    CastorClientException actualCce =
        assertThrows(CastorClientException.class, () -> actualBitTuple.getShare(invalidIndex));
    assertEquals(
        String.format(SHARE_AVAILABLE_FOR_INDEX_EXCEPTION_MSG, invalidIndex),
        actualCce.getMessage());
  }
}
