/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload.websocket;

import io.carbynestack.castor.common.entities.TupleChunk;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Synchronized;

public class ResponseCollector {
  final Map<UUID, ActiveUploadRequest> activeUploadRequests = new HashMap<>();

  static class ActiveUploadRequest extends CountDownLatch {
    private final Object $lock = new Object[0];
    private final List<Boolean> statusList = new ArrayList<>();

    @Synchronized
    public void applyResponse(boolean responseStatus) {
      this.statusList.add(responseStatus);
      this.countDown();
    }

    public boolean getSuccess(long timeout, TimeUnit unit) throws InterruptedException {
      if (this.await(timeout, unit)) {
        synchronized ($lock) {
          return !statusList.isEmpty() && statusList.get(0);
        }
      }
      return false;
    }

    /**
     * Constructs a {@code CountDownLatch} initialized with the given count.
     *
     * @param count the number of times {@link #countDown} must be invoked before threads can pass
     *     through {@link #await}
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public ActiveUploadRequest() {
      super(1);
    }
  }

  /**
   * This method will register / apply a response for an {@link ActiveUploadRequest} for the given
   * chunk id. If no request is associated with the given id, no action will be performed.
   *
   * @param chunkId id of the chunk the response is related to
   * @param responseStatus indicator whether the related upload was successful or not
   */
  public void applyResponse(UUID chunkId, boolean responseStatus) {
    Option.of(this.activeUploadRequests.get(chunkId)).peek(ar -> ar.applyResponse(responseStatus));
  }

  /**
   * Will register a new {@link ActiveUploadRequest} for a given chunk id.
   *
   * <p>If there is already an {@link ActiveUploadRequest} registered with the given id, the
   * existing {@link ActiveUploadRequest} will remain untouched and no action is performed.
   *
   * @param chunkId id of the chunk to be registered
   */
  public void registerUploadRequest(UUID chunkId) {
    activeUploadRequests.putIfAbsent(chunkId, new ActiveUploadRequest());
  }

  /**
   * Waits until either all active upload request for a {@link TupleChunk} with a given id have
   * returned or a specified timeout elapsed.
   *
   * @param chunkId id of the related {@link TupleChunk}
   * @return <i>true</i> if all requests returned successful, or <i>false</i> if either the timeout
   *     elapsed, at least one request has failed, or no {@link ActiveUploadRequest} was registered
   *     for the given chunk id.
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public boolean waitForRequest(UUID chunkId, long timeout, TimeUnit unit)
      throws InterruptedException {
    try {
      return Try.of(() -> activeUploadRequests.get(chunkId).getSuccess(timeout, unit))
          .getOrElse(false);
    } finally {
      activeUploadRequests.remove(chunkId);
    }
  }
}
