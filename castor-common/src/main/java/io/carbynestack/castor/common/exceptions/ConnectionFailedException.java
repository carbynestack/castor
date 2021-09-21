/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.exceptions;

/** Exception thrown in case a WebSocket connection cannot be established. */
public class ConnectionFailedException extends RuntimeException {
  /**
   * Creates a new {@link ConnectionFailedException} with the given message and cause that has led
   * to this exception.
   *
   * @param message The message describing the cause of the exception.
   */
  public ConnectionFailedException(String message) {
    super(message);
  }
}
