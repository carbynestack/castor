/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.websocket;

/**
 * This class provides all resource paths and parameter names as exposed and used by the Castor
 * Service WebSocket interface
 */
public final class CastorWebSocketApiEndpoints {

  /** Websocket endpoint */
  public static final String WEBSOCKET_ENDPOINT = "/intra-vcp/ws";
  /** Prefix for the broker */
  public static final String BROKER_PREFIX = "/queue";
  /** Prefix for the application */
  public static final String APPLICATION_PREFIX = "/app";
  /** Endpoint for the response */
  public static final String RESPONSE_QUEUE_ENDPOINT = BROKER_PREFIX + "/response";
}
