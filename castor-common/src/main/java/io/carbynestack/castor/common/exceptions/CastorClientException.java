/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.exceptions;

/**
 * Exception thrown in case a client triggered operation fails.
 *
 * <p>{@link CastorClientException} are expected to be thrown on invalid client interactions, such
 * as malformed input or invalid requests, rather than service internal errors.
 */
public class CastorClientException extends RuntimeException {

  public CastorClientException(String message) {
    super(message);
  }

  public CastorClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
