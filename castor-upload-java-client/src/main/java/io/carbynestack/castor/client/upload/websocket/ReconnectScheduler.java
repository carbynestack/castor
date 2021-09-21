/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload.websocket;

import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ReconnectScheduler {
  private static final long INITIAL_DELAY = 1000L;
  private static final long PERIODIC_RATE = 5000L;

  private final Timer timer;

  ReconnectScheduler(WebSocketClient socketClient) {
    this(socketClient, INITIAL_DELAY, PERIODIC_RATE);
  }

  ReconnectScheduler(WebSocketClient socketClient, long initialDelay, long periodicRate) {
    log.debug("ReconnectScheduler is created.");
    timer = new Timer(true);
    timer.schedule(new ReconnectTask(socketClient), initialDelay, periodicRate);
  }

  public void stop() {
    timer.cancel();
    log.debug("ReconnectScheduler stopped.");
  }

  static class ReconnectTask extends TimerTask {
    private final WebSocketClient webSocketClient;

    ReconnectTask(WebSocketClient webSocketClient) {
      this.webSocketClient = webSocketClient;
    }

    @Override
    public void run() {
      if (!webSocketClient.isConnected()) {
        log.warn("Connection to cloud lost, try to reconnect...");
        this.webSocketClient.connect();
      } else {
        log.debug("Connection is open.");
      }
    }
  }
}
