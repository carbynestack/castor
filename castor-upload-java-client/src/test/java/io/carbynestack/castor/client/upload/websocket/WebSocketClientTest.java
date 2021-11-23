/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.client.upload.websocket;

import static io.carbynestack.castor.client.upload.websocket.WebSocketClient.*;
import static io.carbynestack.castor.common.entities.Field.GFP;
import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.UPLOAD_TUPLES_ENDPOINT;
import static io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints.APPLICATION_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE;

import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.ConnectionFailedException;
import io.carbynestack.castor.common.exceptions.UnsupportedPayloadException;
import io.carbynestack.castor.common.websocket.UploadTupleChunkResponse;
import io.vavr.control.Option;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import javax.websocket.ContainerProvider;
import javax.websocket.WebSocketContainer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

@ExtendWith(MockitoExtension.class)
class WebSocketClientTest {

  private final CastorServiceUri castorServiceUri =
      new CastorServiceUri("https://castor.carbynestack.io:8080");
  private final int serverHeartbeat = 0;
  private final int clientHeartbeat = 10000;
  private final String bearerToken = "testBearerToken";

  private final WebSocketContainer webSocketContainerMock;
  private final ResponseCollector responseCollectorMock;
  private final StandardWebSocketClient standardWebSocketClientMock;
  private final WebSocketSessionHandler sessionHandlerMock;
  private final WebSocketClient webSocketClient;

  public WebSocketClientTest() throws NoSuchAlgorithmException {
    responseCollectorMock = mock(ResponseCollector.class);
    webSocketContainerMock = mock(WebSocketContainer.class);
    try (MockedStatic<ContainerProvider> csMockedStatic = mockStatic(ContainerProvider.class)) {
      csMockedStatic
          .when(ContainerProvider::getWebSocketContainer)
          .thenReturn(webSocketContainerMock);
      try (MockedConstruction<StandardWebSocketClient> swscConstruction =
          mockConstruction(StandardWebSocketClient.class)) {
        try (MockedConstruction<WebSocketSessionHandler> wsshConstruction =
            mockConstruction(WebSocketSessionHandler.class)) {
          this.webSocketClient =
              new WebSocketClient(
                  castorServiceUri,
                  serverHeartbeat,
                  clientHeartbeat,
                  Option.of(bearerToken),
                  responseCollectorMock);
          this.standardWebSocketClientMock = swscConstruction.constructed().get(0);
          this.sessionHandlerMock = wsshConstruction.constructed().get(0);
        }
      }
    }
  }

  @Test
  void givenBearerTokenDefined_whenConnect_thenConnectWithAdequateHeader() {
    when(standardWebSocketClientMock.doHandshake(
            any(WebSocketHandler.class), any(WebSocketHttpHeaders.class), any(URI.class)))
        .thenReturn(mock(ListenableFuture.class));

    webSocketClient.connect();
    ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
    ArgumentCaptor<WebSocketHttpHeaders> webSocketHttpHeadersArgumentCaptor =
        ArgumentCaptor.forClass(WebSocketHttpHeaders.class);

    verify(standardWebSocketClientMock, times(1))
        .doHandshake(
            any(), webSocketHttpHeadersArgumentCaptor.capture(), uriArgumentCaptor.capture());
    assertEquals(castorServiceUri.getIntraVcpWsServiceUri(), uriArgumentCaptor.getValue());
    WebSocketHttpHeaders actualHeaders = webSocketHttpHeadersArgumentCaptor.getValue();
    assertEquals(
        AUTHORIZATION_HEADER_VALUE_PREFIX + " " + bearerToken,
        actualHeaders.get(AUTHORIZATION_HEADER_NAME).get(0));
  }

  @Test
  void givenConnectionAlreadyEstablished_whenConnect_doNotReinitializeConnection() {
    reset(standardWebSocketClientMock);
    when(sessionHandlerMock.isConnected()).thenReturn(true);

    webSocketClient.connect();

    verifyNoInteractions(standardWebSocketClientMock);
  }

  @Test
  void givenSessionHandlerDefined_whenDisconnect_thenInvokeDisconnect() {
    webSocketClient.disconnect();

    verify(sessionHandlerMock, times(1)).disconnect();
  }

  @Test
  void givenEstablishedNotConnection_whenSend_thenThrowConnectionFailedException() {
    when(webSocketClient.isConnected()).thenReturn(false);
    ConnectionFailedException actualCfe =
        assertThrows(ConnectionFailedException.class, () -> webSocketClient.send(null));

    assertEquals(
        String.format(NOT_CONNECTED_EXCEPTION_MSG, castorServiceUri.getIntraVcpWsServiceUri()),
        actualCfe.getMessage());
  }

  @Test
  void givenEstablishedConnection_whenSend_thenSendExpectedMessage() {
    TupleType tupleType = TupleType.BIT_GFP;
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    byte[] tupleData =
        RandomUtils.nextBytes(
            GFP.getElementSize() * TupleType.BIT_GFP.getArity() * GFP.getElementSize());
    TupleChunk tupleChunk =
        TupleChunk.of(tupleType.getTupleCls(), tupleType.getField(), chunkId, tupleData);
    byte[] expectedSerializedPayload = SerializationUtils.serialize(tupleChunk);
    StompSession stompSessionMock = mock(StompSession.class);

    when(webSocketClient.isConnected()).thenReturn(true);
    when(sessionHandlerMock.getSession()).thenReturn(stompSessionMock);

    webSocketClient.send(tupleChunk);

    ArgumentCaptor<StompHeaders> stompHeadersArgumentCaptor =
        ArgumentCaptor.forClass(StompHeaders.class);
    verify(stompSessionMock, times(1))
        .send(stompHeadersArgumentCaptor.capture(), eq(expectedSerializedPayload));
    StompHeaders actualHeaders = stompHeadersArgumentCaptor.getValue();
    assertEquals(
        APPLICATION_PREFIX + UPLOAD_TUPLES_ENDPOINT,
        actualHeaders.get(StompHeaders.DESTINATION).get(0));
    assertEquals(
        APPLICATION_OCTET_STREAM_VALUE, actualHeaders.get(StompHeaders.CONTENT_TYPE).get(0));
    assertEquals(
        String.valueOf(expectedSerializedPayload.length),
        actualHeaders.get(StompHeaders.CONTENT_LENGTH).get(0));
  }

  @Test
  void givenUnsupportedPayload_whenHandleFrame_thenThrowUnsupportedPayloadException() {
    String unsupportedPayload = "Unsupported payload";
    UnsupportedPayloadException actualUpe =
        assertThrows(
            UnsupportedPayloadException.class,
            () -> webSocketClient.handleFrame(new StompHeaders(), unsupportedPayload));
    assertEquals(INVALID_MESSAGE_PAYLOAD_EXCEPTION_MSG, actualUpe.getMessage());
  }

  @Test
  void givenResponseMessageHasNoChunkIdDefined_whenHandleFrame_thenDiscardMessage() {
    reset(responseCollectorMock);
    UploadTupleChunkResponse responseWithoutId = UploadTupleChunkResponse.failure(null, "failure");
    byte[] payload = SerializationUtils.serialize(responseWithoutId);
    webSocketClient.handleFrame(new StompHeaders(), payload);
    verifyNoInteractions(responseCollectorMock);
  }

  @Test
  void givenValidResponseMessage_whenHandleFrame_thenProcessAccordingly() {
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    UploadTupleChunkResponse successfulResponse = UploadTupleChunkResponse.success(chunkId);
    byte[] payload = SerializationUtils.serialize(successfulResponse);
    webSocketClient.handleFrame(new StompHeaders(), payload);
    verify(responseCollectorMock, times(1)).applyResponse(chunkId, successfulResponse.isSuccess());
  }
}
