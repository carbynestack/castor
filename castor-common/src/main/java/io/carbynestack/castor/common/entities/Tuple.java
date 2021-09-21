/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Interface class for all tuples that are stored in Castor.
 *
 * <p>Tuples are a special type of cryptographic material used for secure execution of SPDZ related
 * operations like secret sharing or distributed multiplications.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Bit.class, name = "Bit"),
  @JsonSubTypes.Type(value = InputMask.class, name = "InputMask"),
  @JsonSubTypes.Type(value = InverseTuple.class, name = "InverseTuple"),
  @JsonSubTypes.Type(value = SquareTuple.class, name = "SquareTuple"),
  @JsonSubTypes.Type(value = MultiplicationTriple.class, name = "MultiplicationTriple")
})
public interface Tuple<T extends Tuple<T, F>, F extends Field> extends Serializable {
  /**
   * Returns the {@link Field} this {@link Tuple} is element of.
   *
   * @return the {@link Field} this {@link Tuple} is element of.
   */
  F getField();

  /**
   * Writes this {@link Tuple} to the given {@link OutputStream}.
   *
   * <p>This method will write one {@link Share} (see {@link #getShares()}) after the other to the
   * given {@link OutputStream}.
   *
   * @param os the stream to write this object to
   * @throws IOException if an I/O error occurs.
   */
  void writeTo(OutputStream os) throws IOException;

  /**
   * Returns the {@link Share}s of this {@link Tuple}
   *
   * @return the {@link Share}s of this {@link Tuple}
   */
  Share[] getShares();
}
