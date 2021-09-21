/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.client.upload.websocket;

import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.RESPONSE_QUEUE_ENDPOINT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.springframework.messaging.simp.stomp.StompSession;

public class WebSocketSessionHandlerTest {
  private WebSocketClient webSocketClientMock;
  private WebSocketSessionHandler webSocketSessionHandler;

  @Before
  public void setup() {
    webSocketClientMock = mock(WebSocketClient.class);
    webSocketSessionHandler = new WebSocketSessionHandler(webSocketClientMock);
  }

  @SneakyThrows
  @Test
  public void
      givenActiveConnectionScheduler_whenAfterConnect_thenSubscribeToResponseMessagesAndDestroyScheduler() {
    Field reconnectSchedulerField =
        WebSocketSessionHandler.class.getDeclaredField("reconnectScheduler");
    reconnectSchedulerField.setAccessible(true);

    ReconnectScheduler reconnectSchedulerMock = mock(ReconnectScheduler.class);
    reconnectSchedulerField.set(webSocketSessionHandler, reconnectSchedulerMock);
    StompSession sessionMock = mock(StompSession.class);

    webSocketSessionHandler.afterConnected(sessionMock, null);

    verify(reconnectSchedulerMock, times(1)).stop();
    verify(sessionMock, times(1)).subscribe(RESPONSE_QUEUE_ENDPOINT, webSocketClientMock);
  }

  @SneakyThrows
  @Test
  public void
      givenSessionIsDisconnected_whenHandleTransportError_thenInitializeConnectionScheduler() {
    Field sessionField = WebSocketSessionHandler.class.getDeclaredField("session");
    sessionField.setAccessible(true);
    StompSession sessionMock = mock(StompSession.class);
    sessionField.set(webSocketSessionHandler, sessionMock);

    when(sessionMock.isConnected()).thenReturn(false);

    try (MockedConstruction<ReconnectScheduler> mockedConstruction =
        mockConstruction(
            ReconnectScheduler.class,
            (context, settings) -> {
              assertEquals(webSocketClientMock, settings.arguments().get(0));
            })) {
      webSocketSessionHandler.handleTransportError(sessionMock, null);

      assertEquals(1, mockedConstruction.constructed().size());
    }
  }
}
