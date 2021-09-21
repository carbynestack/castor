/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.commons.io.IOUtils;

/**
 * {@link ArrayBackedTuple} provides an abstract implementation of {@link Tuple} where the related
 * {@link Share}s are organized in an array.
 *
 * @param <T> Defines the type of {@link Tuple}
 * @param <F> Defines the {@link Field} the {@link Tuple}'s data is element of
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Getter
@ToString
@EqualsAndHashCode
abstract class ArrayBackedTuple<T extends Tuple<T, F>, F extends Field> implements Tuple<T, F> {
  private static final long serialVersionUID = 3454006854456361647L;
  public static final String INVALID_NUMBER_OF_SHARES_EXCEPTION_MSG =
      "Invalid number of shares: %s expects exactly %d shares.";
  public static final String NO_MORE_TUPLE_DATA_AVAILABLE_EXCEPTION_MSG =
      "Unable to read share from stream - No more tuple data available";
  public static final String SHARE_AVAILABLE_FOR_INDEX_EXCEPTION_MSG =
      "No share available for index %d";

  /** The {@link Field} the given {@link Tuple} is element of. */
  F field;
  /** The {@link Share}s of this {@link Tuple} */
  Share[] shares;

  /**
   * Creates a new {@link ArrayBackedTuple} with the given {@link Share}s and {@link Field}.
   *
   * @param field the {@link Field} the given {@link ArrayBackedTuple} is element of
   * @param shares the {@link Share}s of this {@link ArrayBackedTuple}
   * @throws IllegalArgumentException if the number of given shares does not match this {@link
   *     Tuple}'s arity (see {@link TupleType#getArity()}.
   */
  ArrayBackedTuple(F field, Share... shares) {
    int arity = TupleType.arityForTupleClass(this.getClass());
    if (shares.length != arity) {
      throw new IllegalArgumentException(
          String.format(
              INVALID_NUMBER_OF_SHARES_EXCEPTION_MSG, this.getClass().getSimpleName(), arity));
    }
    this.field = field;
    this.shares = shares;
  }

  /**
   * Creates a new {@link ArrayBackedTuple} from an {@link InputStream} for the given {@link Field}.
   *
   * @param field the {@link Field} the given {@link ArrayBackedTuple} is element of
   * @param is the {@link InputStream} to read the data from
   * @throws IOException if the stream does not provide enough data
   */
  ArrayBackedTuple(F field, InputStream is) throws IOException {
    this.field = field;
    int arity = TupleType.arityForTupleClass(this.getClass());
    this.shares = new Share[arity];
    for (int i = 0; i < arity; i++) {
      this.shares[i] = readShareFromStream(is);
    }
  }

  /**
   * Retrieves a specific {@link Share} at the given index.
   *
   * @param index Index of the requested {@link Share}
   * @return The {@link Share} for the given index
   * @throws CastorClientException if no {@link Share} for the given index is available
   */
  public Share getShare(int index) {
    try {
      return this.shares[index];
    } catch (Exception e) {
      throw new CastorClientException(
          String.format(SHARE_AVAILABLE_FOR_INDEX_EXCEPTION_MSG, index), e);
    }
  }

  @Override
  public void writeTo(OutputStream os) throws IOException {
    for (int i = 0; i < this.shares.length; i++) {
      os.write(getShare(i).getValue());
      os.write(getShare(i).getMac());
    }
  }

  private Share readShareFromStream(InputStream inputStream) throws IOException {
    int fieldSize = field.getElementSize();
    try {
      byte[] valueBytes = IOUtils.readFully(inputStream, fieldSize);
      byte[] macBytes = IOUtils.readFully(inputStream, fieldSize);
      return new Share(valueBytes, macBytes);
    } catch (IOException e) {
      throw new IOException(NO_MORE_TUPLE_DATA_AVAILABLE_EXCEPTION_MSG);
    }
  }
}
