/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.websocket;

import org.springframework.messaging.simp.SimpMessageHeaderAccessor;

public interface CastorWebSocketService {
  void uploadTupleChunk(SimpMessageHeaderAccessor headerAccessor, byte[] payload) throws Exception;
}
