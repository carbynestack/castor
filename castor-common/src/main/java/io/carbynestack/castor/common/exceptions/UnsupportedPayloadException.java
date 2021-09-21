/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.exceptions;

/** Exception thrown in case an unexpected payload is received via WebSocket. */
public class UnsupportedPayloadException extends RuntimeException {
  /**
   * Creates a new {@link UnsupportedPayloadException} with the given message and cause that has led
   * to this exception.
   *
   * @param message The message describing the cause of the exception.
   */
  public UnsupportedPayloadException(String message) {
    super(message);
  }
}
