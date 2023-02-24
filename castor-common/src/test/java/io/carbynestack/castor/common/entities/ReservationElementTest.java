/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

class ReservationElementTest {
  private final UUID testChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229");
  private final long testNumberOfTuples = 1200;
  private final long testStartIndex = 42;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void givenChunkIdIsNull_whenCreate_thenThrowNullPointerException() {
    NullPointerException actualNpe =
        assertThrows(
            NullPointerException.class,
            () -> new ReservationElement(null, testNumberOfTuples, testStartIndex));
    assertEquals("tupleChunkId is marked non-null but is null", actualNpe.getMessage());
  }

  @Test
  void givenNumberOfTuplesIsZero_whenCreate_thenThrowIllegalArgumentException() {
    IllegalArgumentException iae =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ReservationElement(testChunkId, 0, testStartIndex));
    assertEquals(ReservationElement.MUST_RESERVE_TUPLES_EXCEPTION_MSG, iae.getMessage());
  }

  @Test
  void givenStartIndexIsNegative_whenCreate_thenThrowIllegalArgumentException() {
    IllegalArgumentException iae =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ReservationElement(testChunkId, testNumberOfTuples, -1));
    assertEquals(
        ReservationElement.START_INDEX_MUST_NOT_BE_NEGATIVE_EXCEPTION_MSG, iae.getMessage());
  }

  @Test
  void givenJsonStringWithoutTupleChunkId_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationElementJsonStringBuilder()
            .withReservedTuples(testNumberOfTuples)
            .withStartIndex(testStartIndex)
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, ReservationElement.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'tupleChunkId'");
  }

  @Test
  void givenJsonStringWithoutNumberOfTripleShares_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationElementJsonStringBuilder()
            .withTupleChunkId(testChunkId)
            .withStartIndex(testStartIndex)
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, ReservationElement.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'reservedTuples'");
  }

  @Test
  void givenJsonStringWithoutStartIndex_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationElementJsonStringBuilder()
            .withTupleChunkId(testChunkId)
            .withReservedTuples(testNumberOfTuples)
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, ReservationElement.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'startIndex'");
  }

  @SneakyThrows
  @Test
  void givenValidJsonString_whenDeserialize_thenReturnExpectedObject() {
    String data =
        new ReservationElementJsonStringBuilder()
            .withTupleChunkId(testChunkId)
            .withReservedTuples(testNumberOfTuples)
            .withStartIndex(testStartIndex)
            .build();

    ReservationElement reservationElement = objectMapper.readValue(data, ReservationElement.class);

    assertEquals(testChunkId, reservationElement.getTupleChunkId());
    assertEquals(testNumberOfTuples, reservationElement.getReservedTuples());
    assertEquals(testStartIndex, reservationElement.getStartIndex());
  }

  private static class ReservationElementJsonStringBuilder {
    private String tupleChunkId;
    private String reservedTuples;
    private String startIndex;

    public ReservationElementJsonStringBuilder withTupleChunkId(UUID tupleChunkId) {
      this.tupleChunkId = String.format("\"tupleChunkId\":\"%s\"", tupleChunkId.toString());
      return this;
    }

    public ReservationElementJsonStringBuilder withReservedTuples(long reservedTuples) {
      this.reservedTuples = String.format("\"reservedTuples\":%d", reservedTuples);
      return this;
    }

    public ReservationElementJsonStringBuilder withStartIndex(long startIndex) {
      this.startIndex = String.format("\"startIndex\":%d", startIndex);
      return this;
    }

    public String build() {
      return String.format(
          "{%s}",
          StringUtils.join(
              Stream.of(tupleChunkId, reservedTuples, startIndex)
                  .filter(Objects::nonNull)
                  .toArray(),
              ','));
    }
  }
}
