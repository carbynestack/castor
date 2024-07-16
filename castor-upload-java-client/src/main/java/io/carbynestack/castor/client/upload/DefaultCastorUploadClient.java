/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload;

import static org.springframework.util.Assert.isTrue;

import io.carbynestack.castor.client.upload.websocket.ResponseCollector;
import io.carbynestack.castor.client.upload.websocket.WebSocketClient;
import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.httpclient.BearerTokenUtils;
import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.io.File;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/**
 * The default implementation of a {@link CastorUploadClient}. It can be used to upload {@link
 * TupleChunk}s to one ore more castor services.
 */
@Slf4j
public class DefaultCastorUploadClient implements CastorUploadClient {
  public static final int MAXIMUM_NUMBER_OF_TUPLES = 1000000;
  public static final int DEFAULT_SERVER_HEARTBEAT = 0;
  public static final int DEFAULT_CLIENT_HEARTBEAT = 10000;
  public static final String FAILED_TO_INSTANTIATE_HTTP_CLIENT_EXCEPTION_MSG =
      "Failed to instantiate HttpClient.";
  public static final String INITIALIZE_WEB_SOCKET_CLIENT_FAILED_EXCEPTION_MSG =
      "Initializing WebSocketClient failed.";
  public static final String TOO_MANY_TUPLES_EXCEPTION_MSG =
      "The maximum number of tuples is exceeded: actual=%d, allowed=%d";
  public static final String FAILED_ACTIVATE_TUPLE_CHUNK_EXCEPTION_MSG =
      "Failed to activate TupleChunk";
  public static final String CONNECTION_TIMEOUT_EXCEPTION_MSG =
      "WebSocket connection to CastorServices timed out.";
  public static final String WEB_SOCKET_CONNECTION_FAILED_EXCEPTION_MSG =
      "WebSocket connection to CastorServices failed";
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final ResponseCollector responseCollector = new ResponseCollector();
  private final Option<BearerTokenProvider> bearerTokenProvider;
  private final CsHttpClient<String> csHttpClient;
  final CastorServiceUri castorServiceUri;
  final WebSocketClient webSocketClient;

  /**
   * @throws CastorClientException if initializing the http client failed for the given
   *     configuration
   * @throws CastorClientException if initializing the websocket clients failed for the given
   *     configuration
   */
  protected DefaultCastorUploadClient(Builder builder) {
    this.castorServiceUri = builder.getCastorServiceUri();
    this.bearerTokenProvider = Option.of(builder.getBearerTokenProvider());
    csHttpClient =
        Try.of(
                () ->
                    CsHttpClient.<String>builder()
                        .withFailureType(String.class)
                        .withoutSslValidation(builder.isNoSslValidation())
                        .withTrustedCertificates(builder.getTrustedCertificates())
                        .build())
            .getOrElseThrow(
                t -> new CastorClientException(FAILED_TO_INSTANTIATE_HTTP_CLIENT_EXCEPTION_MSG, t));
    webSocketClient =
        initializeWebSocketClient(
            castorServiceUri,
            builder.getServerHeartbeat(),
            builder.getClientHeartbeat(),
            bearerTokenProvider);
  }

  protected DefaultCastorUploadClient(
      Builder builder, CsHttpClient<String> csHttpClient, WebSocketClient webSocketClient) {
    this.csHttpClient = csHttpClient;
    this.webSocketClient = webSocketClient;
    this.castorServiceUri = builder.getCastorServiceUri();
    this.bearerTokenProvider = Option.of(builder.getBearerTokenProvider());
  }

  @Override
  public boolean uploadTupleChunk(TupleChunk tupleChunk) {
    return uploadTupleChunk(tupleChunk, DEFAULT_CONNECTION_TIMEOUT);
  }

  @Override
  public boolean uploadTupleChunk(TupleChunk tupleChunk, long timeout) {
    isTrue(
        tupleChunk.getNumberOfTuples() <= MAXIMUM_NUMBER_OF_TUPLES,
        String.format(
            TOO_MANY_TUPLES_EXCEPTION_MSG,
            tupleChunk.getNumberOfTuples(),
            MAXIMUM_NUMBER_OF_TUPLES));
    responseCollector.registerUploadRequest(tupleChunk.getChunkId());
    webSocketClient.send(tupleChunk);
    try {
      return responseCollector.waitForRequest(
          tupleChunk.getChunkId(), timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.error("Client got interrupted or timed out while uploading tuple chunk", e);
      Thread.currentThread().interrupt();
      return false;
    } finally {
      ((ThreadPoolTaskScheduler) Objects.requireNonNull(webSocketClient.getTaskScheduler()))
          .shutdown();
    }
  }

  @Override
  public void activateTupleChunk(UUID tupleChunkId) {
    URI requestUri = castorServiceUri.getIntraVcpActivateTupleChunkResourceUri(tupleChunkId);
    try {
      csHttpClient.put(
          requestUri,
          bearerTokenProvider
              .flatMap(provider -> Option.of(provider.apply(castorServiceUri)))
              .map(BearerTokenUtils::createBearerToken)
              .toJavaList(),
          null);
    } catch (CsHttpClientException e) {
      log.debug(FAILED_ACTIVATE_TUPLE_CHUNK_EXCEPTION_MSG, e);
      throw new CastorClientException(FAILED_ACTIVATE_TUPLE_CHUNK_EXCEPTION_MSG, e);
    }
  }

  @Override
  public void disconnectWebSocket() {
    webSocketClient.disconnect();
  }

  @Override
  public void connectWebSocket(long timeout) {
    Future<?> awaitConnection =
        executor.submit(
            () -> {
              webSocketClient.connect();
              while (!webSocketClient.isConnected() && !Thread.interrupted()) {
                log.debug("Waiting for WebSocket connection..");
                try {
                  TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                }
              }
            });
    try {
      awaitConnection.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException e) {
      log.debug(CONNECTION_TIMEOUT_EXCEPTION_MSG, e);
    } finally {
      awaitConnection.cancel(true);
    }
    if (!webSocketClient.isConnected()) {
      throw new CastorClientException(WEB_SOCKET_CONNECTION_FAILED_EXCEPTION_MSG);
    }
  }

  WebSocketClient initializeWebSocketClient(
      CastorServiceUri castorServiceUri,
      int serverHeartbeat,
      int clientHeartbeat,
      Option<BearerTokenProvider> bearerTokenProvider) {
    try {
      return WebSocketClient.of(
          castorServiceUri,
          serverHeartbeat,
          clientHeartbeat,
          bearerTokenProvider.flatMap(provider -> Option.of(provider.apply(castorServiceUri))),
          responseCollector);
    } catch (NoSuchAlgorithmException nsae) {
      throw new CastorClientException(INITIALIZE_WEB_SOCKET_CLIENT_FAILED_EXCEPTION_MSG, nsae);
    }
  }

  /**
   * Create a new {@link Builder} to easily configure and create a new {@link CastorUploadClient}
   *
   * @param serviceAddress Address of the service the new {@link DefaultCastorUploadClient} should
   *     communicate with.
   */
  public static Builder builder(@NonNull String serviceAddress) {
    return new Builder(serviceAddress);
  }

  /** Builder class to create a new {@link DefaultCastorUploadClient}. */
  @Getter
  public static class Builder {
    private final CastorServiceUri castorServiceUri;
    private int serverHeartbeat = DEFAULT_SERVER_HEARTBEAT;
    private int clientHeartbeat = DEFAULT_CLIENT_HEARTBEAT;
    private final List<File> trustedCertificates;
    private boolean noSslValidation = false;
    private BearerTokenProvider bearerTokenProvider;

    /**
     * Create a new {@link DefaultCastorUploadClient.Builder} to easily configure and create a new
     * {@link DefaultCastorUploadClient}.
     *
     * @param serviceAddress Address of the service the new {@link DefaultCastorUploadClient} should
     *     communicate with.
     * @throws NullPointerException if given serviceAddresses is null
     * @throws IllegalArgumentException if given serviceAddresses is empty
     * @throws IllegalArgumentException if a single given serviceAddress is null or empty
     */
    private Builder(String serviceAddress) {
      this.castorServiceUri = new CastorServiceUri(serviceAddress);
      this.trustedCertificates = new ArrayList<>();
    }

    /**
     * Disables the SSL certificate validation check.
     *
     * <p>
     *
     * <p><b>WARNING</b><br>
     * Please be aware, that this option leads to insecure web connections and is meant to be used
     * in a local test setup only. Using this option in a productive environment is explicitly
     * <u>not recommended</u>.
     */
    public Builder withoutSslCertificateValidation() {
      this.noSslValidation = true;
      return this;
    }

    /**
     * Adds a certificate (.pem) to the trust store.<br>
     * This allows tls secured communication with services that do not have a certificate issued by
     * an official CA (certificate authority).
     *
     * @param trustedCertificate Public certificate.
     */
    public Builder withTrustedCertificate(File trustedCertificate) {
      this.trustedCertificates.add(trustedCertificate);
      return this;
    }

    /**
     * Sets the server side connection heart-beat {@link
     * WebSocketStompClient#setDefaultHeartbeat(long[])}.
     *
     * @param serverHeartbeat heart-beat in milliseconds. Default is <CODE>0</CODE>
     */
    public Builder withServerHeartbeat(int serverHeartbeat) {
      this.serverHeartbeat = serverHeartbeat;
      return this;
    }

    /**
     * Sets the client side connection heart-beat {@link
     * WebSocketStompClient#setDefaultHeartbeat(long[])}.
     *
     * @param clientHeartbeat heart-beat in milliseconds. Default is <CODE>10000</CODE>
     */
    public Builder withClientHeartbeat(int clientHeartbeat) {
      this.clientHeartbeat = clientHeartbeat;
      return this;
    }

    /**
     * Sets a provider for getting a backend specific bearer token that is injected as an
     * authorization header to REST HTTP calls emitted by the client.
     *
     * @param bearerTokenProvider Bearer token provider to be injected
     */
    public Builder withBearerTokenProvider(BearerTokenProvider bearerTokenProvider) {
      this.bearerTokenProvider = bearerTokenProvider;
      return this;
    }

    /**
     * Builds and returns a new {@link DefaultCastorUploadClient} according to the given
     * configuration.
     *
     * @throws CastorClientException If the CastorUploadClient could not be instantiated.
     */
    public DefaultCastorUploadClient build() {
      return new DefaultCastorUploadClient(this);
    }
  }
}
