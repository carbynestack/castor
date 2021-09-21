/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload.websocket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.client.upload.websocket.ReconnectScheduler.ReconnectTask;
import java.util.Timer;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReconnectSchedulerTest {

  @SneakyThrows
  @Test
  public void givenDisconnectionClient_whenCreate_thenInitializeTimerAndRetryConnection() {
    long expectedDelay = 0L;
    long expectedRate = 1L;
    WebSocketClient webSocketClientMock = mock(WebSocketClient.class);
    try (MockedConstruction<Timer> timerMockedConstruction = mockConstruction(Timer.class)) {
      try (MockedConstruction<ReconnectTask> reconnectTaskMockedConstruction =
          mockConstruction(
              ReconnectTask.class,
              (context, settings) ->
                  assertEquals(webSocketClientMock, settings.arguments().get(0)))) {
        ReconnectScheduler reconnectScheduler =
            new ReconnectScheduler(webSocketClientMock, expectedDelay, expectedRate);
        Timer actualTimerMock = timerMockedConstruction.constructed().get(0);
        verify(actualTimerMock).schedule(any(), eq(expectedDelay), eq(expectedRate));
        reconnectScheduler.stop();
        verify(actualTimerMock).cancel();
      }
    }
  }

  @SneakyThrows
  @Test
  public void givenDisconnectionClient_whenRunReconnectTask_thenCallConnect() {
    WebSocketClient webSocketClientMock = mock(WebSocketClient.class);
    when(webSocketClientMock.isConnected()).thenReturn(false);
    ReconnectTask reconnectTask = new ReconnectTask(webSocketClientMock);
    reconnectTask.run();
    verify(webSocketClientMock).isConnected();
    verify(webSocketClientMock).connect();
  }

  @SneakyThrows
  @Test
  public void givenConnectedClient_whenRunReconnectTask_thenDoNothing() {
    WebSocketClient webSocketClientMock = mock(WebSocketClient.class);
    when(webSocketClientMock.isConnected()).thenReturn(true);
    ReconnectTask reconnectTask = new ReconnectTask(webSocketClientMock);
    reconnectTask.run();
    verify(webSocketClientMock).isConnected();
    verifyNoMoreInteractions(webSocketClientMock);
  }
}
