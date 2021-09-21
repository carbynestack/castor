/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.io.Serializable;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * A chunk of {@link Tuple}s represented by a byte array and identified by a unique id. <br>
 * Used for uploading multiple tuples at once.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TupleChunk<T extends Tuple<T, F>, F extends Field> implements Serializable {
  private static final long serialVersionUID = -5025431542939631318L;
  public static final String INVALID_DATA_LENGTH_EXCEPTION_MSG =
      "Length of TupleChunk data must be a multiple of %s bytes!";

  UUID chunkId;
  TupleType tupleType;
  byte[] tuples;

  /**
   * Returns a new {@link TupleChunk} with the given data.
   *
   * @param tupleCls The class of {@link Tuple}s stored in the new {@link TupleChunk}.
   * @param field Defines the {@link Field} the mathematical operations take place
   * @param chunkId Id of the {@link TupleChunk} used to match related {@link Tuple} shares in the
   *     Carbyne Stack MPC Cluster.
   * @param tuples The actually {@link Tuple} data
   * @return a new {@link TupleChunk} object
   * @throws CastorClientException if the given tuple data is invalid
   */
  public static <T extends Tuple<T, F>, F extends Field> TupleChunk<T, F> of(
      Class<T> tupleCls, F field, @NonNull UUID chunkId, byte[] tuples) {
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    verifyTuples(tupleType, tuples);
    return new TupleChunk<>(chunkId, tupleType, tuples);
  }

  /**
   * Verifies whether the given tuples byte array contains valid data (length).
   *
   * @param tuples the tuples as byte array
   * @throws CastorClientException if the given tuple data is not valid
   */
  private static void verifyTuples(TupleType tupleType, byte[] tuples) {
    int arity = tupleType.getArity();
    if (tuples.length % (arity * tupleType.getShareSize()) != 0) {
      throw new CastorClientException(
          String.format(
              INVALID_DATA_LENGTH_EXCEPTION_MSG, arity * tupleType.getField().getElementSize()));
    }
  }

  /**
   * Returns the number of {@link Tuple}s stored in this {@link TupleChunk}.
   *
   * @return the number of {@link Tuple}s stored in this {@link TupleChunk}.
   */
  public int getNumberOfTuples() {
    int arity = tupleType.getArity();
    return this.getTuples().length / (arity * tupleType.getShareSize());
  }
}
