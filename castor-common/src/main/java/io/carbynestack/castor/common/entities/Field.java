/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.annotation.*;
import java.io.Serializable;
import lombok.*;
import lombok.experimental.FieldDefaults;

/**
 * A {@link Field} is an algebraic structure and defines mathematical field the related resource is
 * element of.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Field.Gf2n.class, name = "Gf2n"),
  @JsonSubTypes.Type(value = Field.Gfp.class, name = "Gfp")
})
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Field implements Serializable {
  private static final long serialVersionUID = -1418465461566891261L;

  /**
   * A static {@link Field} instance representing a GF(p) field (ring) where <i>p</i> defines the
   * number of elements in the field.
   */
  public static final Gfp GFP = new Gfp("gfp", 16);
  /** A static {@link Field} instance representing a GF(2^n) field. */
  public static final Gf2n GF2N = new Gf2n("gf2n", 8);

  @JsonTypeName("Gfp")
  public static final class Gfp extends Field {
    @JsonCreator
    public Gfp(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "elementSize", required = true) int elementSize) {
      super(name, elementSize);
    }
  }

  @JsonTypeName("Gf2n")
  public static final class Gf2n extends Field {

    @JsonCreator
    public Gf2n(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "elementSize", required = true) int elementSize) {
      super(name, elementSize);
    }
  }

  /** Name identifier of the field. */
  @JsonProperty(required = true)
  String name;
  /** Byte size of a {@link Share}'s value and mac in the given {@link Field} */
  @JsonProperty(required = true)
  int elementSize;
}
