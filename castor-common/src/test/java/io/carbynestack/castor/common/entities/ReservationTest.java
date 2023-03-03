/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static io.carbynestack.castor.common.entities.Reservation.ID_MUST_NOT_BE_NULL_OR_EMPTY_EXCEPTION_MSG;
import static io.carbynestack.castor.common.entities.Reservation.ONE_RESERVATION_ELEMENT_REQUIRED_EXCEPTION_MSG;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import java.util.*;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReservationTest {
  private final String testReservationId =
      "80fbba1b-3da8-4b1e-8a2c-cebd65229fad_MULTIPLICATION_TRIPLE_GFP";
  private final TupleType testTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
  private final ReservationElement testReservationElement =
      new ReservationElement(UUID.fromString("80fbba1b-3da8-4b1e-0123-456789abcdef"), 42, 0);
  private final List<ReservationElement> testReservationElements =
      Collections.singletonList(testReservationElement);

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void givenIdIsNull_whenCreate_thenThrowNullPointerException() {
    NullPointerException actualNpe =
        assertThrows(
            NullPointerException.class,
            () -> new Reservation(null, testTupleType, testReservationElements));
    assertEquals("reservationId is marked non-null but is null", actualNpe.getMessage());
  }

  @Test
  void givenIdIsEmpty_whenCreate_thenThrowIllegalArgumentException() {
    IllegalArgumentException iae =
        assertThrows(
            IllegalArgumentException.class,
            () -> new Reservation("", testTupleType, testReservationElements));
    assertEquals(ID_MUST_NOT_BE_NULL_OR_EMPTY_EXCEPTION_MSG, iae.getMessage());
  }

  @Test
  void givenReservationElementsIsEmpty_whenCreate_thenThrowIllegalArgumentException() {
    List<ReservationElement> emptyReservations = emptyList();
    IllegalArgumentException iae =
        assertThrows(
            IllegalArgumentException.class,
            () -> new Reservation(testReservationId, testTupleType, emptyReservations));
    assertEquals(ONE_RESERVATION_ELEMENT_REQUIRED_EXCEPTION_MSG, iae.getMessage());
  }

  @SneakyThrows
  @Test
  void givenValidJsonString_whenDeserialize_thenReturnExpectedObject() {
    ActivationStatus expectedStatus = ActivationStatus.UNLOCKED;
    String jsonString =
        new ReservationJsonStringBuilder()
            .withReservationId(testReservationId)
            .withTupleType(testTupleType)
            .withStatus(expectedStatus)
            .withReservations(testReservationElements)
            .build();
    Reservation actualReservation = objectMapper.readValue(jsonString, Reservation.class);
    assertEquals(testReservationId, actualReservation.getReservationId());
    assertEquals(testTupleType, actualReservation.getTupleType());
    assertEquals(expectedStatus, actualReservation.getStatus());
    assertEquals(testReservationElements, actualReservation.getReservations());
  }

  @Test
  void givenJsonStringWithoutReservationId_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationJsonStringBuilder()
            .withTupleType(TupleType.MULTIPLICATION_TRIPLE_GFP)
            .withStatus(ActivationStatus.LOCKED)
            .withReservations(new ArrayList<>())
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, Reservation.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'reservationId'");
  }

  @Test
  void givenJsonStringWithoutReservations_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationJsonStringBuilder()
            .withReservationId(UUID.randomUUID().toString())
            .withTupleType(TupleType.MULTIPLICATION_TRIPLE_GFP)
            .withStatus(ActivationStatus.LOCKED)
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, Reservation.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'reservations'");
  }

  @Test
  void givenJsonStringWithoutTupleType_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationJsonStringBuilder()
            .withReservationId(UUID.randomUUID().toString())
            .withStatus(ActivationStatus.LOCKED)
            .withReservations(new ArrayList<>())
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, Reservation.class));
    assertThat(actualJpeMie.getMessage())
        .startsWith("Missing required creator property 'tupleType'");
  }

  @Test
  void givenJsonStringWithoutStatus_whenDeserialize_thenThrowJsonParseException() {
    String incompleteData =
        new ReservationJsonStringBuilder()
            .withReservationId(UUID.randomUUID().toString())
            .withTupleType(TupleType.MULTIPLICATION_TRIPLE_GFP)
            .withReservations(new ArrayList<>())
            .build();

    MismatchedInputException actualJpeMie =
        assertThrows(
            MismatchedInputException.class,
            () -> objectMapper.readValue(incompleteData, Reservation.class));
    assertThat(actualJpeMie.getMessage()).startsWith("Missing required creator property 'status'");
  }

  private static class ReservationJsonStringBuilder {
    private String reservationId;
    private String tupleType;
    private String reservations;
    private String status;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ReservationJsonStringBuilder withReservationId(String requestId) {
      this.reservationId = String.format("\"reservationId\":\"%s\"", requestId);
      return this;
    }

    public ReservationJsonStringBuilder withTupleType(TupleType tupleType) {
      this.tupleType = String.format("\"tupleType\":\"%s\"", tupleType.name());
      return this;
    }

    public ReservationJsonStringBuilder withStatus(ActivationStatus status) {
      this.status = String.format("\"status\":\"%s\"", status.name());
      return this;
    }

    @SneakyThrows
    public ReservationJsonStringBuilder withReservations(List<ReservationElement> reservations) {
      this.reservations =
          String.format("\"reservations\":%s", objectMapper.writeValueAsString(reservations));
      return this;
    }

    public String build() {
      return String.format(
          "{%s}",
          StringUtils.join(
              Stream.of(reservationId, tupleType, status, reservations)
                  .filter(Objects::nonNull)
                  .toArray(),
              ','));
    }
  }
}
