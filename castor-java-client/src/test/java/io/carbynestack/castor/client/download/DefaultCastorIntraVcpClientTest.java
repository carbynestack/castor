/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import static io.carbynestack.castor.client.download.DefaultCastorIntraVcpClient.FAILED_DOWNLOADING_TUPLES_EXCEPTION_MSG;
import static io.carbynestack.castor.client.download.DefaultCastorIntraVcpClient.FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG;
import static io.carbynestack.castor.common.CastorServiceUri.MUST_NOT_BE_EMPTY_EXCEPTION_MSG;
import static io.carbynestack.castor.common.entities.Field.GFP;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import io.carbynestack.httpclient.CsResponseEntity;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCastorIntraVcpClientTest {
  private final Share testShare =
      new Share(
          new byte[] {1, 4, 72, -56, -22, -12, 72, 88, 109, 42, -27, 56, 109, 42, -27, 56},
          new byte[] {1, 4, 72, -56, -22, -12, 72, 88, 109, 42, -27, 56, 109, 42, -27, 56});

  private final CsHttpClient<String> csHttpClientMock;
  private final String serviceAddress = "https://castor.carbynestack.io:8080";

  private final CastorIntraVcpClient castorIntraVcpClient;

  public DefaultCastorIntraVcpClientTest() {
    csHttpClientMock = mock(CsHttpClient.class);
    castorIntraVcpClient =
        new DefaultCastorIntraVcpClient(
            DefaultCastorIntraVcpClient.builder(serviceAddress), csHttpClientMock);
  }

  @Test
  public void givenServiceAddressIsNull_whenGetBuilderInstance_thenThrowIllegalArgumentException() {
    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () -> DefaultCastorIntraVcpClient.builder(null).build());
    assertEquals(MUST_NOT_BE_EMPTY_EXCEPTION_MSG, actualIae.getMessage());
  }

  @SneakyThrows
  @Test
  public void givenSslConfiguration_whenBuildClient_thenInitializeCsHttpClientAccordingly() {
    CastorServiceUri serviceUri = new CastorServiceUri(serviceAddress);
    String expectedBearerToken = "testBearerToken";
    BearerTokenProvider expectedBearerTokenProvider =
        BearerTokenProvider.builder().bearerToken(serviceUri, expectedBearerToken).build();
    File expectedTrustedCertificateFile =
        Files.createTempFile("testCertificateFile", "pem").toFile();
    try (MockedConstruction<CsHttpClient> mockedConstruction =
        Mockito.mockConstruction(
            CsHttpClient.class,
            (csHttpClient1, context) -> {
              assertEquals(true, context.arguments().get(2));
              assertEquals(
                  singletonList(expectedTrustedCertificateFile), context.arguments().get(3));
            })) {
      DefaultCastorIntraVcpClient actualCastorDownloadClient =
          DefaultCastorIntraVcpClient.builder(serviceAddress)
              .withoutSslCertificateValidation()
              .withBearerTokenProvider(expectedBearerTokenProvider)
              .withTrustedCertificate(expectedTrustedCertificateFile)
              .build();
      assertEquals(
          new CastorServiceUri(serviceAddress), actualCastorDownloadClient.getServiceUri());
      assertEquals(
          expectedBearerTokenProvider, actualCastorDownloadClient.getBearerTokenProvider().get());
    }
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenDownloadTripleShares_thenReturnExpectedContent() {
    UUID requestId = UUID.fromString("3dc08ff2-5eed-49a9-979e-3a3ac0e4a2cf");
    int expectedCount = 2;
    TupleList<MultiplicationTriple<Field.Gfp>, Field.Gfp> expectedTripleList =
        new TupleList(MultiplicationTriple.class, GFP);
    expectedTripleList.add(new MultiplicationTriple(GFP, testShare, testShare, testShare));
    expectedTripleList.add(new MultiplicationTriple(GFP, testShare, testShare, testShare));
    CsResponseEntity<String, TupleList> givenResponseEntity =
        CsResponseEntity.success(HttpStatus.SC_OK, expectedTripleList);
    CastorServiceUri serviceUri = new CastorServiceUri(serviceAddress);

    when(csHttpClientMock.getForEntity(
            serviceUri.getIntraVcpRequestTuplesUri(
                requestId, TupleType.MULTIPLICATION_TRIPLE_GFP, expectedCount),
            Collections.emptyList(),
            TupleList.class))
        .thenReturn(givenResponseEntity);
    TupleList actualTripleList =
        castorIntraVcpClient.downloadTupleShares(
            requestId, TupleType.MULTIPLICATION_TRIPLE_GFP, expectedCount);

    assertEquals(expectedTripleList, actualTripleList);
  }

  @SneakyThrows
  @Test
  public void givenRequestEmitsError_whenDownloadTripleShares_thenThrowCastorClientException() {
    UUID requestId = UUID.fromString("3dc08ff2-5eed-49a9-979e-3a3ac0e4a2cf");
    CsHttpClientException expectedCause = new CsHttpClientException("totally expected");
    URI expectedUri = new CastorServiceUri(serviceAddress).getRestServiceUri();
    when(csHttpClientMock.getForEntity(any(), eq(Collections.emptyList()), eq(TupleList.class)))
        .thenThrow(expectedCause);
    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () ->
                castorIntraVcpClient.downloadTupleShares(
                    requestId, TupleType.MULTIPLICATION_TRIPLE_GFP, 1));

    assertEquals(
        String.format(
            FAILED_DOWNLOADING_TUPLES_EXCEPTION_MSG,
            expectedUri.toString(),
            expectedCause.getMessage()),
        actualCce.getMessage());
    assertEquals(expectedCause, actualCce.getCause());
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenGetTelemetryData_thenReturnExpectedContent() {
    TelemetryData expectedTelemetryData = new TelemetryData(new ArrayList<>(), 100);
    CsResponseEntity<String, TelemetryData> responseEntity =
        CsResponseEntity.success(HttpStatus.SC_OK, expectedTelemetryData);
    when(csHttpClientMock.getForEntity(
            new CastorServiceUri(serviceAddress).getIntraVcpTelemetryUri(),
            Collections.emptyList(),
            TelemetryData.class))
        .thenReturn(responseEntity);
    TelemetryData actualTelemetryData = castorIntraVcpClient.getTelemetryData();
    assertEquals(expectedTelemetryData, actualTelemetryData);
  }

  @SneakyThrows
  @Test
  public void givenRequestEmitsError_whenGetTelemetryData_thenThrowCastorClientException() {
    CsHttpClientException expectedCause = new CsHttpClientException("totally expected");
    CastorServiceUri serviceUri = new CastorServiceUri(serviceAddress);
    when(csHttpClientMock.getForEntity(
            serviceUri.getIntraVcpTelemetryUri(), Collections.emptyList(), TelemetryData.class))
        .thenThrow(expectedCause);

    CastorClientException actualCce =
        assertThrows(CastorClientException.class, castorIntraVcpClient::getTelemetryData);

    assertEquals(
        String.format(
            FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG,
            serviceUri.getRestServiceUri(),
            expectedCause.getMessage()),
        actualCce.getMessage());
    assertEquals(expectedCause, actualCce.getCause());
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenGetTelemetryDataWithInterval_thenReturnExpectedContent() {
    long requestInterval = 50;
    TelemetryData expectedTelemetryData = new TelemetryData(new ArrayList<>(), 100);
    CsResponseEntity<String, TelemetryData> responseEntity =
        CsResponseEntity.success(HttpStatus.SC_OK, expectedTelemetryData);
    when(csHttpClientMock.getForEntity(
            new CastorServiceUri(serviceAddress).getRequestTelemetryUri(requestInterval),
            Collections.emptyList(),
            TelemetryData.class))
        .thenReturn(responseEntity);
    TelemetryData actualTelemetryData = castorIntraVcpClient.getTelemetryData(requestInterval);
    assertEquals(expectedTelemetryData, actualTelemetryData);
  }

  @SneakyThrows
  @Test
  public void
      givenRequestEmitsError_whenGetTelemetryDataWithInterval_thenThrowCastorClientException() {
    long requestInterval = 50;
    CsHttpClientException expectedCause = new CsHttpClientException("totally expected");

    CastorServiceUri castorServiceUri = new CastorServiceUri(serviceAddress);
    when(csHttpClientMock.getForEntity(
            castorServiceUri.getRequestTelemetryUri(requestInterval),
            Collections.emptyList(),
            TelemetryData.class))
        .thenThrow(expectedCause);

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> castorIntraVcpClient.getTelemetryData(requestInterval));

    assertEquals(
        String.format(
            FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG,
            castorServiceUri.getRestServiceUri(),
            expectedCause.getMessage()),
        actualCce.getMessage());
    assertEquals(expectedCause, actualCce.getCause());
  }
}
