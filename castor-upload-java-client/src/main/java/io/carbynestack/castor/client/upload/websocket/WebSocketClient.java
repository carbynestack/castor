/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload.websocket;

import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.UPLOAD_TUPLES_ENDPOINT;
import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.APPLICATION_PREFIX;
import static org.apache.tomcat.websocket.Constants.SSL_CONTEXT_PROPERTY;
import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE;

import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.ConnectionFailedException;
import io.carbynestack.castor.common.exceptions.UnsupportedPayloadException;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import io.vavr.control.Option;
import java.lang.reflect.Type;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.websocket.ContainerProvider;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

@Slf4j
public class WebSocketClient extends WebSocketStompClient implements StompFrameHandler {

  public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
  public static final String AUTHORIZATION_HEADER_VALUE_PREFIX = "Bearer";
  public static final String INVALID_MESSAGE_PAYLOAD_EXCEPTION_MSG =
      "Received invalid message payload.";
  public static final String NOT_CONNECTED_EXCEPTION_MSG = "Connection not established for \"%s\"";
  private final ResponseCollector responseCollector;
  private final WebSocketSessionHandler sessionHandler;
  private final CastorServiceUri castorServiceUri;
  private final Option<String> bearerToken;

  WebSocketClient(
      CastorServiceUri castorServiceUri,
      int serverHeartbeat,
      int clientHeartbeat,
      Option<String> bearerToken,
      ResponseCollector responseCollector)
      throws NoSuchAlgorithmException {
    super(getStandardWebSocketClient());
    this.responseCollector = responseCollector;
    this.castorServiceUri = castorServiceUri;
    this.bearerToken = bearerToken;
    this.sessionHandler = new WebSocketSessionHandler(this);
    long[] heartbeat = {serverHeartbeat, clientHeartbeat};
    this.setDefaultHeartbeat(heartbeat);
    this.setTaskScheduler(createTaskScheduler());
  }

  public static WebSocketClient of(
      CastorServiceUri castorServiceUri,
      int serverHeartbeat,
      int clientHeartbeat,
      Option<String> bearerToken,
      ResponseCollector responseCollector)
      throws NoSuchAlgorithmException {
    return new WebSocketClient(
        castorServiceUri, serverHeartbeat, clientHeartbeat, bearerToken, responseCollector);
  }

  public void connect() {
    if (!isConnected()) {
      WebSocketHttpHeaders headers = new WebSocketHttpHeaders();
      bearerToken.peek(
          t -> headers.add(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_HEADER_VALUE_PREFIX + " " + t));
      this.connect(castorServiceUri.getIntraVcpWsServiceUri().toString(), headers, sessionHandler);
    }
  }

  public void disconnect() {
    this.sessionHandler.disconnect();
  }

  public boolean isConnected() {
    return this.sessionHandler.isConnected();
  }

  /**
   * A wrapper to the clients {@link StompSession} : {@link StompSession#send(StompHeaders, Object)}
   *
   * @param payload the message payload
   * @throws ConnectionFailedException if the websocket connection has not been established yet
   */
  public void send(TupleChunk payload) {
    if (!isConnected()) {
      throw new ConnectionFailedException(
          String.format(NOT_CONNECTED_EXCEPTION_MSG, castorServiceUri.getIntraVcpWsServiceUri()));
    }
    byte[] serializedPayload = SerializationUtils.serialize(payload);
    StompHeaders headers = new StompHeaders();
    headers.add(StompHeaders.DESTINATION, APPLICATION_PREFIX + UPLOAD_TUPLES_ENDPOINT);
    headers.add(StompHeaders.CONTENT_TYPE, APPLICATION_OCTET_STREAM_VALUE);
    headers.add(StompHeaders.CONTENT_LENGTH, String.valueOf(serializedPayload.length));
    this.sessionHandler.getSession().send(headers, serializedPayload);
    log.info(
        String.format(
            "TupleChunk with %d %s was sent to CastorService.",
            payload.getNumberOfTuples(), payload.getTupleType()));
  }

  private static StandardWebSocketClient getStandardWebSocketClient()
      throws NoSuchAlgorithmException {
    StandardWebSocketClient standardWebSocketClient =
        new StandardWebSocketClient(ContainerProvider.getWebSocketContainer());
    standardWebSocketClient.getUserProperties().put(SSL_CONTEXT_PROPERTY, SSLContext.getDefault());
    return standardWebSocketClient;
  }

  private TaskScheduler createTaskScheduler() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.initialize();
    return taskScheduler;
  }

  @NonNull
  @Override
  public Type getPayloadType(@NonNull StompHeaders headers) {
    return byte[].class;
  }

  /**
   * @throws CastorClientException if message payload cannot be parsed as {@link
   *     UploadTupleChunkResponse}.
   */
  @Override
  public void handleFrame(@NonNull StompHeaders headers, Object payload) {
    if (payload instanceof byte[]) {
      UploadTupleChunkResponse response = UploadTupleChunkResponse.fromPayload((byte[]) payload);
      handleResponse(response);
    } else {
      log.debug("Received invalid message payload: {}", payload.toString());
      throw new UnsupportedPayloadException(INVALID_MESSAGE_PAYLOAD_EXCEPTION_MSG);
    }
  }

  private void handleResponse(UploadTupleChunkResponse responseEntity) {
    if (responseEntity.isSuccess()) {
      log.info("Response success response for UUID {}", responseEntity.getChunkId());
    } else {
      log.error(
          "Error received for UUID {}: {}",
          responseEntity.getChunkId(),
          responseEntity.getErrorMsg());
    }
    if (responseEntity.getChunkId() != null) {
      responseCollector.applyResponse(responseEntity.getChunkId(), responseEntity.isSuccess());
    }
  }
}
