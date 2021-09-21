/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload.websocket;

import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;

@Slf4j
public class WebSocketSessionHandler extends StompSessionHandlerAdapter {

  private StompSession session;
  private final WebSocketClient webSocketClient;
  private ReconnectScheduler reconnectScheduler;

  WebSocketSessionHandler(WebSocketClient webSocketClient) {
    this.webSocketClient = webSocketClient;
  }

  @Override
  public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
    log.debug("Opened new session with ID {}", session.getSessionId());
    this.session = session;
    destroyConnectionScheduler();

    session.subscribe(RESPONSE_QUEUE_ENDPOINT, webSocketClient);
    log.debug("Subscription added for destination {}", RESPONSE_QUEUE_ENDPOINT);
  }

  @Override
  public void handleTransportError(StompSession session, Throwable exception) {
    log.error("An error occurred in session with ID " + session.getSessionId(), exception);
    if (!this.session.isConnected()) {
      initializeConnectionScheduler();
    }
  }

  @Override
  public void handleException(
      StompSession session,
      StompCommand command,
      StompHeaders headers,
      byte[] payload,
      Throwable exception) {
    log.error("An error occurred", exception);
  }

  @Override
  public void handleFrame(StompHeaders headers, Object payload) {
    // no need to do anything
  }

  StompSession getSession() {
    return session;
  }

  boolean isConnected() {
    return (session != null && session.isConnected());
  }

  void disconnect() {
    destroyConnectionScheduler();
    if (session != null) {
      session.disconnect();
      session = null;
    }
  }

  private void initializeConnectionScheduler() {
    if (reconnectScheduler == null) {
      reconnectScheduler = new ReconnectScheduler(webSocketClient);
    }
  }

  private void destroyConnectionScheduler() {
    if (reconnectScheduler != null) {
      reconnectScheduler.stop();
      reconnectScheduler = null;
    }
  }
}
