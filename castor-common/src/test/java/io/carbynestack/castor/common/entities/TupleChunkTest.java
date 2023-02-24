/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.common.entities.TupleChunk.INVALID_DATA_LENGTH_EXCEPTION_MSG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

class TupleChunkTest {

  @Test
  void givenValidAttributes_whenCreate_thenReturnExpectedChunk() {
    TupleType expectedTupleType = TupleType.BIT_GFP;
    UUID expectedUUID = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    byte[] expectedTupleData =
        RandomUtils.nextBytes(
            GFP.getElementSize() * TupleType.BIT_GFP.getArity() * GFP.getElementSize());
    TupleChunk actualTupleChunk =
        TupleChunk.of(
            expectedTupleType.getTupleCls(),
            expectedTupleType.getField(),
            expectedUUID,
            expectedTupleData);
    assertEquals(expectedTupleType, actualTupleChunk.getTupleType());
    assertEquals(expectedUUID, actualTupleChunk.getChunkId());
    assertEquals(expectedTupleData, actualTupleChunk.getTuples());
  }

  @Test
  void givenDataOfInvalidLength_whenCreate_thenThrowCastorClientException() {
    TupleType tupleType = TupleType.BIT_GFP;
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    byte[] invalidTupleData =
        RandomUtils.nextBytes(
            GFP.getElementSize() * TupleType.BIT_GFP.getArity() * GFP.getElementSize() + 1);
    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () ->
                TupleChunk.of(
                    tupleType.getTupleCls(), tupleType.getField(), chunkId, invalidTupleData));
    assertEquals(
        String.format(
            INVALID_DATA_LENGTH_EXCEPTION_MSG,
            tupleType.getArity() * tupleType.getField().getElementSize()),
        actualCce.getMessage());
  }
}
