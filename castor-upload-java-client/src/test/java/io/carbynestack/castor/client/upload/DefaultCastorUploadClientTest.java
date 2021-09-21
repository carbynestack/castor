/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.upload;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
import java.io.File;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.http.Header;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCastorUploadClientTest {

  private WebSocketClient webSocketClientMock;
  private BearerTokenProvider bearerTokenProviderMock;
  private CsHttpClient<String> csHttpClientMock;
  private ResponseCollector responseCollectorMock;
  private final CastorServiceUri serviceUri =
      new CastorServiceUri("https://castor.carbynestack.io:8080");
  private final int clientHeartbeat = 100;
  private final int serverHeartbeat = 200;

  private DefaultCastorUploadClient castorUploadClient;

  @SneakyThrows
  @Before
  public void setUp() {
    csHttpClientMock = mock(CsHttpClient.class);
    webSocketClientMock = mock(WebSocketClient.class);
    bearerTokenProviderMock = mock(BearerTokenProvider.class);

    DefaultCastorUploadClient.Builder uploadClientBuilderMock =
        mock(DefaultCastorUploadClient.Builder.class);
    when(uploadClientBuilderMock.getCastorServiceUri()).thenReturn(serviceUri);
    when(uploadClientBuilderMock.getBearerTokenProvider()).thenReturn(bearerTokenProviderMock);
    try (MockedConstruction<ResponseCollector> responseCollectorMockedConstruction =
        mockConstruction(ResponseCollector.class)) {
      castorUploadClient =
          new DefaultCastorUploadClient(
              uploadClientBuilderMock, csHttpClientMock, webSocketClientMock);
      responseCollectorMock = responseCollectorMockedConstruction.constructed().get(0);
    }
  }

  @Test
  public void givenServiceAddressesIsNull_whenGetBuilderInstance_thenThrowNullPointerException() {
    NullPointerException actualIae =
        assertThrows(
            NullPointerException.class, () -> DefaultCastorUploadClient.builder(null).build());
    assertEquals("serviceAddress is marked non-null but is null", actualIae.getMessage());
  }

  @SneakyThrows
  @Test
  public void givenConfiguration_whenBuildClient_thenInitializeAccordingly() {
    CastorServiceUri expectedServiceUri =
        new CastorServiceUri("https://castor.carbynestack.io:8080");
    int expectedServerHeartbeat = 100;
    int expectedClientHeartbeat = 200;
    List<File> expectedTrustedCertificates =
        Lists.newArrayList(
            Files.createTempFile("testCertificateFile", "pem").toFile(),
            Files.createTempFile("testCertificateFile", "pem").toFile());
    BearerTokenProvider expectedBearerTokenProvider = BearerTokenProvider.builder().build();

    DefaultCastorUploadClient.Builder builder =
        DefaultCastorUploadClient.builder(expectedServiceUri.getRestServiceUri().toString())
            .withClientHeartbeat(expectedClientHeartbeat)
            .withServerHeartbeat(expectedServerHeartbeat)
            .withoutSslCertificateValidation()
            .withBearerTokenProvider(expectedBearerTokenProvider);
    expectedTrustedCertificates.forEach(builder::withTrustedCertificate);
    try (MockedConstruction<DefaultCastorUploadClient> clientMockedConstruction =
        mockConstruction(
            DefaultCastorUploadClient.class,
            (context, settings) -> {
              DefaultCastorUploadClient.Builder actualBuilder =
                  (DefaultCastorUploadClient.Builder) settings.arguments().get(0);
              assertEquals(expectedServiceUri, actualBuilder.getCastorServiceUri());
              assertEquals(expectedClientHeartbeat, actualBuilder.getClientHeartbeat());
              assertEquals(expectedServerHeartbeat, actualBuilder.getServerHeartbeat());
              assertEquals(expectedBearerTokenProvider, actualBuilder.getBearerTokenProvider());
              assertEquals(expectedTrustedCertificates, actualBuilder.getTrustedCertificates());
              assertTrue(actualBuilder.isNoSslValidation());
            })) {
      builder.build();
      assertEquals(1, clientMockedConstruction.constructed().size());
    }
  }

  @SneakyThrows
  @Test
  public void givenHttpClientConstructionFails_whenConstruct_thenThrowCastorClientException() {
    CsHttpClientException expectedCause = new CsHttpClientException("totally Expected");
    DefaultCastorUploadClient.Builder uploadClientBuilderMock =
        mock(DefaultCastorUploadClient.Builder.class, RETURNS_SELF);
    CsHttpClient.CsHttpClientBuilder httpBuilderMock =
        mock(CsHttpClient.CsHttpClientBuilder.class, RETURNS_SELF);
    try (MockedStatic<CsHttpClient> httpClientMockedStatic = mockStatic(CsHttpClient.class)) {
      httpClientMockedStatic.when(CsHttpClient::builder).thenReturn(httpBuilderMock);
      doThrow(expectedCause).when(httpBuilderMock).build();
      CastorClientException actualCce =
          assertThrows(
              CastorClientException.class,
              () -> new DefaultCastorUploadClient(uploadClientBuilderMock));
      assertEquals(
          DefaultCastorUploadClient.FAILED_TO_INSTANTIATE_HTTP_CLIENT_EXCEPTION_MSG,
          actualCce.getMessage());
      assertEquals(expectedCause, actualCce.getCause());
    }
  }

  @SneakyThrows
  @Test
  public void givenWebSocketClientConstructionFails_whenConstruct_thenThrowCastorClientException() {
    NoSuchAlgorithmException expectedCause = new NoSuchAlgorithmException("totally Expected");
    DefaultCastorUploadClient.Builder uploadClientBuilderMock =
        mock(DefaultCastorUploadClient.Builder.class, RETURNS_SELF);
    CsHttpClient.CsHttpClientBuilder httpBuilderMock =
        mock(CsHttpClient.CsHttpClientBuilder.class, RETURNS_SELF);
    try (MockedStatic<CsHttpClient> httpClientMockedStatic = mockStatic(CsHttpClient.class)) {
      httpClientMockedStatic.when(CsHttpClient::builder).thenReturn(httpBuilderMock);
      try (MockedStatic<WebSocketClient> webSocketClientMockedStatic =
          mockStatic(WebSocketClient.class)) {
        when(uploadClientBuilderMock.getCastorServiceUri()).thenReturn(serviceUri);
        when(uploadClientBuilderMock.getServerHeartbeat()).thenReturn(serverHeartbeat);
        when(uploadClientBuilderMock.getClientHeartbeat()).thenReturn(clientHeartbeat);
        when(uploadClientBuilderMock.getBearerTokenProvider()).thenReturn(bearerTokenProviderMock);
        when(bearerTokenProviderMock.apply(serviceUri)).thenReturn(null);
        webSocketClientMockedStatic
            .when(
                () ->
                    WebSocketClient.of(
                        eq(serviceUri),
                        eq(serverHeartbeat),
                        eq(clientHeartbeat),
                        eq(Option.none()),
                        any(ResponseCollector.class)))
            .thenThrow(expectedCause);
        CastorClientException actualCce =
            assertThrows(
                CastorClientException.class,
                () -> new DefaultCastorUploadClient(uploadClientBuilderMock));
        assertEquals(
            DefaultCastorUploadClient.INITIALIZE_WEB_SOCKET_CLIENT_FAILED_EXCEPTION_MSG,
            actualCce.getMessage());
        assertEquals(expectedCause, actualCce.getCause());
      }
    }
  }

  @Test
  public void givenToManyTuples_whenUploadTupleChunk_thenThrowIllegalArgumentException() {
    TupleChunk largeTupleChunk = mock(TupleChunk.class);
    when(largeTupleChunk.getNumberOfTuples())
        .thenReturn(DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES + 1);
    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> castorUploadClient.uploadTupleChunk(largeTupleChunk));
    assertEquals(
        String.format(
            DefaultCastorUploadClient.TOO_MANY_TUPLES_EXCEPTION_MSG,
            DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES + 1,
            DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES),
        actualIae.getMessage());
  }

  @SneakyThrows
  @Test
  public void givenUploadTimesOut_whenUploadTupleChunk_thenReturnFalse() {
    boolean expectedReturnValue = false;
    UUID tupleChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleChunk tupleChunk = mock(TupleChunk.class);
    when(tupleChunk.getNumberOfTuples())
        .thenReturn(DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES);
    when(tupleChunk.getChunkId()).thenReturn(tupleChunkId);
    ThreadPoolTaskScheduler taskSchedulerMock = mock(ThreadPoolTaskScheduler.class);
    when(webSocketClientMock.getTaskScheduler()).thenReturn(taskSchedulerMock);
    when(responseCollectorMock.waitForRequest(
            tupleChunkId, CastorUploadClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        .thenReturn(expectedReturnValue);

    boolean actualReturnValue = castorUploadClient.uploadTupleChunk(tupleChunk);
    assertEquals(expectedReturnValue, actualReturnValue);
  }

  @SneakyThrows
  @Test
  public void givenThreadIsInterrupted_whenUploadTupleChunk_thenReturnFalse() {
    boolean expectedReturnValue = false;
    UUID tupleChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleChunk tupleChunk = mock(TupleChunk.class);
    when(tupleChunk.getNumberOfTuples())
        .thenReturn(DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES);
    when(tupleChunk.getChunkId()).thenReturn(tupleChunkId);
    ThreadPoolTaskScheduler taskSchedulerMock = mock(ThreadPoolTaskScheduler.class);
    when(webSocketClientMock.getTaskScheduler()).thenReturn(taskSchedulerMock);
    when(responseCollectorMock.waitForRequest(
            tupleChunkId, CastorUploadClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        .thenThrow(new InterruptedException("expected"));

    boolean actualReturnValue = castorUploadClient.uploadTupleChunk(tupleChunk);
    assertEquals(expectedReturnValue, actualReturnValue);
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenUploadTupleChunk_thenReturnTrue() {
    UUID tupleChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleChunk tupleChunk = mock(TupleChunk.class);
    when(tupleChunk.getNumberOfTuples())
        .thenReturn(DefaultCastorUploadClient.MAXIMUM_NUMBER_OF_TUPLES);
    when(tupleChunk.getChunkId()).thenReturn(tupleChunkId);
    ThreadPoolTaskScheduler taskSchedulerMock = mock(ThreadPoolTaskScheduler.class);
    when(webSocketClientMock.getTaskScheduler()).thenReturn(taskSchedulerMock);
    when(responseCollectorMock.waitForRequest(
            tupleChunkId, CastorUploadClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        .thenReturn(true);

    boolean actualReturnValue = castorUploadClient.uploadTupleChunk(tupleChunk);
    assertEquals(true, actualReturnValue);
  }

  @SneakyThrows
  @Test
  public void
      givenActivationFailsForOneEndpoint_whenActivateTupleChunk_thenThrowCastorClientException() {
    UUID tupleChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    String bearerToken = "testBearerToken";
    when(bearerTokenProviderMock.apply(serviceUri)).thenReturn(bearerToken);
    CsHttpClientException expectedCause = new CsHttpClientException("expected");
    ArgumentCaptor<List> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
    doThrow(expectedCause)
        .when(csHttpClientMock)
        .put(
            eq(serviceUri.getIntraVcpActivateTupleChunkResourceUri(tupleChunkId)),
            listArgumentCaptor.capture(),
            eq(null));
    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class, () -> castorUploadClient.activateTupleChunk(tupleChunkId));
    assertEquals(
        DefaultCastorUploadClient.FAILED_ACTIVATE_TUPLE_CHUNK_EXCEPTION_MSG,
        actualCce.getMessage());
    assertEquals(expectedCause, actualCce.getCause());
    Header actualHeader = ((List<Header>) listArgumentCaptor.getValue()).get(0);
    assertEquals(actualHeader.getName(), BearerTokenUtils.createBearerToken(bearerToken).getName());
    assertEquals(
        actualHeader.getValue(), BearerTokenUtils.createBearerToken(bearerToken).getValue());
  }

  @Test
  public void givenSuccessfulRequest_whenDisconnectWebSocket_thenCallDisconnectOnEachClient() {
    castorUploadClient.disconnectWebSocket();
    verify(webSocketClientMock).disconnect();
  }

  @Test
  public void givenConnectionAttemptTimesOut_whenConnectWebSocket_thenThrowCastorClientException() {
    when(webSocketClientMock.isConnected()).thenReturn(false);
    CastorClientException actualCce =
        assertThrows(CastorClientException.class, () -> castorUploadClient.connectWebSocket(100));
    verify(webSocketClientMock).connect();
    verify(webSocketClientMock, atLeastOnce()).isConnected();
    assertEquals(
        DefaultCastorUploadClient.WEB_SOCKET_CONNECTION_FAILED_EXCEPTION_MSG,
        actualCce.getMessage());
  }

  @Test
  public void givenConnectionSuccessful_whenConnectWebSocket_thenDoNothing() {
    when(webSocketClientMock.isConnected()).thenReturn(true);
    castorUploadClient.connectWebSocket(50);
    verify(webSocketClientMock).connect();
    verify(webSocketClientMock, times(2)).isConnected();
    verifyNoMoreInteractions(webSocketClientMock);
  }

  @SneakyThrows
  @Test
  public void givenValidConfiguration_whenInitializeWebSocketClients_thenReturnClients() {
    String bearerToken = "testBearerToken";
    try (MockedStatic<WebSocketClient> webSocketClientMockedStatic =
        mockStatic(WebSocketClient.class)) {
      when(bearerTokenProviderMock.apply(serviceUri)).thenReturn(bearerToken);
      webSocketClientMockedStatic
          .when(
              () ->
                  WebSocketClient.of(
                      eq(serviceUri),
                      eq(serverHeartbeat),
                      eq(clientHeartbeat),
                      eq(Option.some(bearerToken)),
                      any(ResponseCollector.class)))
          .thenReturn(webSocketClientMock);
      WebSocketClient actualWebSocketClient =
          castorUploadClient.initializeWebSocketClient(
              serviceUri, serverHeartbeat, clientHeartbeat, Option.some(bearerTokenProviderMock));
      assertEquals(webSocketClientMock, actualWebSocketClient);
    }
  }
}
