/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.exceptions;

/**
 * Exception thrown in case an Castor Service operation fails.
 *
 * <p>{@link CastorServiceException} indicates that internal operations have failed, e.g., when
 * dependencies could not be resolved or are incorrect configuration.
 */
public class CastorServiceException extends RuntimeException {

  /**
   * Creates a new {@link CastorServiceException} with the given message.
   *
   * @param message The message describing the cause of the exception.
   */
  public CastorServiceException(String message) {
    super(message);
  }

  /**
   * Creates a new {@link CastorServiceException} with the given message and cause that has led to
   * this exception.
   *
   * @param message The message describing the cause of the exception.
   * @param cause The cause that has lead to this exception providing further details.
   */
  public CastorServiceException(String message, Throwable cause) {
    super(message, cause);
  }
}
