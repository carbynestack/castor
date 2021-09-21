/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.UUID;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * A {@link ReservationElement} is part of a {@link Reservation} and provides information about the
 * number of reserved tuple shares and their position in a referenced {@link TupleChunk}.
 */
@Value
@Accessors(chain = true)
public class ReservationElement implements Serializable {
  private static final long serialVersionUID = -5294934943093478981L;
  public static final String START_INDEX_MUST_NOT_BE_NEGATIVE_EXCEPTION_MSG =
      "StartIndex must not be negative.";
  public static final String MUST_RESERVE_TUPLES_EXCEPTION_MSG =
      "At least 1 tuple share needs to be reserved.";

  /** Id of the {@link TupleChunk} this {@link ReservationElement} refers to */
  @NonNull UUID tupleChunkId;

  /** Number of tuple shares reserved by this {@link ReservationElement} */
  long reservedTuples;

  /**
   * Index of first reserved tuple share in the referenced {@link TupleChunk} Starting at position
   * 0.
   */
  long startIndex;

  /**
   * Creates a new {@link ReservationElement}
   *
   * @param tupleChunkId id of the referenced {@link TupleChunk}
   * @param reservedTuples NNumber of tuple shares reserved by this {@link ReservationElement}
   * @param startIndex index of first reserved tuple share in referenced {@link TupleChunk},
   *     starting at index 0
   * @throws NullPointerException if the tupleChunkId is <i>null</i>
   * @throws IllegalArgumentException if numberOfTupleShares is less than 1
   * @throws IllegalArgumentException if startIndex is negative
   */
  @JsonCreator
  public ReservationElement(
      @NonNull @JsonProperty(value = "tupleChunkId", required = true) UUID tupleChunkId,
      @JsonProperty(value = "reservedTuples", required = true) long reservedTuples,
      @JsonProperty(value = "startIndex", required = true) long startIndex) {
    if (reservedTuples < 1) {
      throw new IllegalArgumentException(MUST_RESERVE_TUPLES_EXCEPTION_MSG);
    }
    if (startIndex < 0) {
      throw new IllegalArgumentException(START_INDEX_MUST_NOT_BE_NEGATIVE_EXCEPTION_MSG);
    }
    this.tupleChunkId = tupleChunkId;
    this.reservedTuples = reservedTuples;
    this.startIndex = startIndex;
  }
}
