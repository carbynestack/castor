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
import lombok.Value;

/** A share is an element of a {@link Tuple} and consists of a value and a mac. */
@Value(staticConstructor = "of")
public class Share implements Serializable {
  private static final long serialVersionUID = -641361730366133653L;
  public static final String DATA_MUST_BE_SAME_LENGTH_EXCEPTION_MESSAGE =
      "Value and mac must be of same length.";

  /** The actual value of the share. */
  byte[] value;
  /** The mac value of the share. */
  byte[] mac;

  @JsonCreator
  public Share(
      @JsonProperty(value = "value", required = true) byte[] value,
      @JsonProperty(value = "mac", required = true) byte[] mac) {
    if (value.length != mac.length) {
      throw new IllegalArgumentException(DATA_MUST_BE_SAME_LENGTH_EXCEPTION_MESSAGE);
    }
    this.value = value;
    this.mac = mac;
  }
}
