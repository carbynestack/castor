/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static io.carbynestack.castor.common.entities.Field.GF2N;
import static io.carbynestack.castor.common.entities.Field.GFP;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/** An enumeration which defines the available types of {@link Tuple}s and their characteristics. */
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public enum TupleType {
  BIT_GFP("bit_gfp", Bit.class, GFP, 1),
  BIT_GF2N("bit_gf2n", Bit.class, GF2N, 1),
  INPUT_MASK_GFP("inputmask_gfp", InputMask.class, GFP, 1),
  INPUT_MASK_GF2N("inputmask_gf2n", InputMask.class, GF2N, 1),
  INVERSE_TUPLE_GFP("inversetuple_gfp", InverseTuple.class, GFP, 2),
  INVERSE_TUPLE_GF2N("inversetuple_gf2n", InverseTuple.class, GF2N, 2),
  SQUARE_TUPLE_GFP("squaretuple_gfp", SquareTuple.class, GFP, 2),
  SQUARE_TUPLE_GF2N("squaretuple_gf2n", SquareTuple.class, GF2N, 2),
  MULTIPLICATION_TRIPLE_GFP("multiplicationtriple_gfp", MultiplicationTriple.class, GFP, 3),
  MULTIPLICATION_TRIPLE_GF2N("multiplicationtriple_gf2n", MultiplicationTriple.class, GF2N, 3);

  /** Name identifier for the given {@link TupleType} */
  String tupleName;
  /** The class of {@link Tuple} this {@link TupleType} describes */
  @Getter @JsonIgnore transient Class<? extends Tuple> tupleCls;
  /** The {@link Field} the {@link Tuple}s of the given {@link TupleType} are element of */
  @Getter @JsonIgnore transient Field field;
  /** Number of shares stored by a {@link Tuple} of the given {@link TupleType} */
  @Getter @JsonIgnore int arity;

  /**
   * Gets the size in bytes of a share respectively element in a tuple
   *
   * @return the size of a share of this tuple
   */
  public final int getShareSize() {
    return getField().getElementSize() * 2; // value and mac key
  }

  /**
   * Gets the size in bytes of the tuple.
   *
   * @return the size of this tuple
   */
  public final int getTupleSize() {
    return this.getArity() * this.getShareSize();
  }

  @Override
  public String toString() {
    return tupleName;
  }

  /**
   * Returns the {@link TupleType} that is described by the given {@link Tuple Tuple class} and
   * {@link Field}
   *
   * @param tupleCls The {@link Tuple Tuple class} that describes the requested {@link TupleType}
   * @param field The {@link Field} the {@link Tuple}s of the requested {@link TupleType} are
   *     element of
   * @return The {@link TupleType} described by the given parameters
   * @throws CastorClientException if none of the defined {@link TupleType}s can be described by the
   *     given parameters
   */
  public static TupleType findTupleType(Class<? extends Tuple> tupleCls, Field field)
      throws CastorClientException {
    return Arrays.stream(TupleType.values())
        .filter(type -> (type.getTupleCls().equals(tupleCls) && type.getField().equals(field)))
        .findFirst()
        .orElseThrow(
            () ->
                new CastorClientException(
                    String.format(
                        "No TupleType found for Class %s and Field %s",
                        tupleCls.getSimpleName(), field)));
  }

  /**
   * Returns the number of {@link Share}s that are stored by a {@link Tuple} with the given class.
   *
   * @param tupleCls The {@link Tuple} class the number of stored {@link Share}s is requested for
   * @return number of {@link Share}s stored by the given {@link Tuple} class
   */
  public static int arityForTupleClass(Class<? extends Tuple> tupleCls) {
    return Arrays.stream(TupleType.values())
        .filter(type -> type.getTupleCls().equals(tupleCls))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("No TupleType declared for Class %s", tupleCls.getSimpleName())))
        .arity;
  }
}
