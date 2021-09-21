/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.client.upload.websocket;

import static org.junit.Assert.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.Test;

public class ResponseCollectorTest {

  @Test
  public void givenActiveUploadRequestForId_whenRegisterUploadRequest_thenDoNothing() {
    ResponseCollector responseCollector = new ResponseCollector();
    UUID existingRegisteredChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    ResponseCollector.ActiveUploadRequest existingActiveUploadRequest =
        new ResponseCollector.ActiveUploadRequest();
    responseCollector.activeUploadRequests.put(
        existingRegisteredChunkId, existingActiveUploadRequest);
    responseCollector.registerUploadRequest(existingRegisteredChunkId);
    assertEquals(1, responseCollector.activeUploadRequests.size());
    assertEquals(
        existingActiveUploadRequest,
        responseCollector.activeUploadRequests.get(existingRegisteredChunkId));
    assertEquals(
        1, responseCollector.activeUploadRequests.get(existingRegisteredChunkId).getCount());
  }

  @Test
  public void
      givenNoActiveUploadRegisteredForId_whenRegisterUploadRequest_thenCreateNewActiveUploadRequest() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    ResponseCollector responseCollector = new ResponseCollector();

    responseCollector.registerUploadRequest(chunkId);

    ResponseCollector.ActiveUploadRequest actualActiveUploadRequest =
        responseCollector.activeUploadRequests.get(chunkId);
    assertEquals(1, actualActiveUploadRequest.getCount());
  }

  @Test
  public void givenNoActiveUploadRequestForId_whenApplyResponse_thenDoNothing() {
    ResponseCollector responseCollector = new ResponseCollector();
    UUID unregisteredChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    responseCollector.applyResponse(unregisteredChunkId, true);
    assertTrue(responseCollector.activeUploadRequests.isEmpty());
  }

  @SneakyThrows
  @Test
  public void givenNoActiveUploadRequestForId_whenWaitForRequest_thenReturnFalse() {
    ResponseCollector responseCollector = new ResponseCollector();
    UUID unregisteredChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    assertFalse(responseCollector.waitForRequest(unregisteredChunkId, 0, TimeUnit.MILLISECONDS));
  }

  @SneakyThrows
  @Test
  public void givenUploadRequestTimesOut_whenWaitForRequest_thenReturnFalse() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    ResponseCollector responseCollector = new ResponseCollector();
    responseCollector.registerUploadRequest(chunkId);

    assertFalse(responseCollector.waitForRequest(chunkId, 0, TimeUnit.MILLISECONDS));
  }

  @SneakyThrows
  @Test
  public void givenRequestReturnedFalse_whenWaitForRequest_thenReturnFalse() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    ResponseCollector.ActiveUploadRequest givenActiveUploadRequest =
        new ResponseCollector.ActiveUploadRequest();
    givenActiveUploadRequest.applyResponse(false);
    ResponseCollector responseCollector = new ResponseCollector();
    responseCollector.activeUploadRequests.put(chunkId, givenActiveUploadRequest);

    assertFalse(responseCollector.waitForRequest(chunkId, 0, TimeUnit.MILLISECONDS));
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulUploadRequest_whenWaitForRequest_thenReturnSuccess() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    ResponseCollector.ActiveUploadRequest givenActiveUploadRequest =
        new ResponseCollector.ActiveUploadRequest();
    givenActiveUploadRequest.applyResponse(true);
    ResponseCollector responseCollector = new ResponseCollector();
    responseCollector.activeUploadRequests.put(chunkId, givenActiveUploadRequest);
    assertTrue(responseCollector.waitForRequest(chunkId, 0, TimeUnit.MILLISECONDS));
  }
}
