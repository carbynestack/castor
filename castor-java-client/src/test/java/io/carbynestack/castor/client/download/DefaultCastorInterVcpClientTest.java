/*
 * Copyright (c) 2023 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import static io.carbynestack.castor.common.CastorServiceUri.MUST_NOT_BE_EMPTY_EXCEPTION_MSG;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import io.carbynestack.httpclient.CsResponseEntity;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultCastorInterVcpClientTest {

  private final CsHttpClient<String> csHttpClientMock;
  private final List<String> serviceAddresses =
      Arrays.asList("https://castor.carbynestack.io:8080", "https://castor.carbynestack.io:8081");

  private final CastorInterVcpClient castorInterVcpClient;

  public DefaultCastorInterVcpClientTest() {
    csHttpClientMock = mock(CsHttpClient.class);
    castorInterVcpClient =
        new DefaultCastorInterVcpClient(
            DefaultCastorInterVcpClient.builder(serviceAddresses), csHttpClientMock);
  }

  @Test
  void givenServiceAddressIsNull_whenGetBuilderInstance_thenThrowIllegalArgumentException() {
    NullPointerException actualNpe =
        assertThrows(NullPointerException.class, () -> DefaultCastorInterVcpClient.builder(null));
    assertEquals("serviceAddresses is marked non-null but is null", actualNpe.getMessage());
  }

  @Test
  void givenSingleServiceAddressIsNull_whenGetBuilderInstance_thenThrowIllegalArgumentException() {
    List<String> listWithNullAddress = singletonList(null);
    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> DefaultCastorInterVcpClient.builder(listWithNullAddress));
    assertEquals(MUST_NOT_BE_EMPTY_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  void givenSingleServiceAddressIsEmpty_whenGetBuilderInstance_thenThrowIllegalArgumentException() {
    List<String> listWithEmptyAddress = singletonList("");
    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> DefaultCastorInterVcpClient.builder(listWithEmptyAddress));
    assertEquals(MUST_NOT_BE_EMPTY_EXCEPTION_MSG, actualIae.getMessage());
  }

  @SneakyThrows
  @Test
  void givenSslConfiguration_whenBuildClient_thenInitializeCsHttpClientAccordingly() {
    String expectedBearerToken = "testBearerToken";
    BearerTokenProvider expectedBearerTokenProvider = mock(BearerTokenProvider.class);
    File expectedTrustedCertificateFile = mock(File.class);
    try (MockedConstruction<CsHttpClient> mockedConstruction =
        Mockito.mockConstruction(
            CsHttpClient.class,
            (csHttpClient1, context) -> {
              assertEquals(true, context.arguments().get(2));
              assertEquals(
                  singletonList(expectedTrustedCertificateFile), context.arguments().get(3));
            })) {
      DefaultCastorInterVcpClient actualInterVcpClient =
          DefaultCastorInterVcpClient.builder(serviceAddresses)
              .withoutSslCertificateValidation()
              .withBearerTokenProvider(expectedBearerTokenProvider)
              .withTrustedCertificate(expectedTrustedCertificateFile)
              .build();
      assertEquals(
          serviceAddresses.stream().map(CastorServiceUri::new).collect(Collectors.toList()),
          actualInterVcpClient.getServiceUris());
      assertEquals(
          expectedBearerTokenProvider, actualInterVcpClient.getBearerTokenProvider().get());
    }
  }

  @SneakyThrows
  @Test
  void givenSingleEndpointReturnsFailure_whenShareReservation_thenReturnFalse() {
    URI successEndpoint = new CastorServiceUri(serviceAddresses.get(0)).getInterVcpReservationUri();
    URI failureEndpoint = new CastorServiceUri(serviceAddresses.get(1)).getInterVcpReservationUri();
    Reservation sharedReservation = mock(Reservation.class);
    CsResponseEntity<String, String> expectedSuccessResponse =
        CsResponseEntity.success(200, "success");
    CsResponseEntity<String, String> expectedFailureResponse =
        CsResponseEntity.failed(404, "failure");

    when(csHttpClientMock.postForEntity(failureEndpoint, sharedReservation, String.class))
        .thenReturn(expectedFailureResponse);
    when(csHttpClientMock.postForEntity(successEndpoint, sharedReservation, String.class))
        .thenReturn(expectedSuccessResponse);

    assertFalse(castorInterVcpClient.shareReservation(sharedReservation));
  }

  @SneakyThrows
  @Test
  void givenCommunicationFails_whenShareReservation_thenThrowCastorClientException() {
    CsHttpClientException expectedException = new CsHttpClientException("expected");
    Reservation sharedReservation = mock(Reservation.class);

    when(csHttpClientMock.postForEntity(any(), any(), any())).thenThrow(expectedException);

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> castorInterVcpClient.shareReservation(sharedReservation));
    assertEquals(
        DefaultCastorInterVcpClient.FAILED_SHARING_RESERVATION_EXCEPTION_MSG,
        actualCce.getMessage());
    assertEquals(expectedException, actualCce.getCause());
  }

  @SneakyThrows
  @Test
  void givenSuccessfulRequest_whenShareReservation_thenReturnTrue() {
    Reservation sharedReservation = mock(Reservation.class);
    CsResponseEntity<String, String> successResponse = CsResponseEntity.success(200, "success");

    for (String address : serviceAddresses) {
      when(csHttpClientMock.postForEntity(
              new CastorServiceUri(address).getInterVcpReservationUri(),
              sharedReservation,
              String.class))
          .thenReturn(successResponse);
    }

    assertTrue(castorInterVcpClient.shareReservation(sharedReservation));
  }

  @SneakyThrows
  @Test
  void givenCommunicationFails_whenUpdateReservation_thenThrowCastorClientException() {
    CsHttpClientException expectedException = new CsHttpClientException("expected");
    String reservationId = "reservationId";
    ActivationStatus status = ActivationStatus.UNLOCKED;

    doThrow(expectedException).when(csHttpClientMock).put(any(), eq(status));

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> castorInterVcpClient.updateReservationStatus(reservationId, status));
    assertEquals(
        DefaultCastorInterVcpClient.FAILED_UPDATING_RESERVATION_EXCEPTION_MSG,
        actualCce.getMessage());
    assertEquals(expectedException, actualCce.getCause());
  }

  @SneakyThrows
  @Test
  void givenSuccessfulRequests_whenUpdateReservation_thenDoNothing() {
    String reservationId = "reservationId";
    ActivationStatus status = ActivationStatus.UNLOCKED;

    castorInterVcpClient.updateReservationStatus(reservationId, status);
    for (String address : serviceAddresses) {
      verify(csHttpClientMock)
          .put(
              new CastorServiceUri(address).getInterVcpUpdateReservationUri(reservationId), status);
    }
  }
}
