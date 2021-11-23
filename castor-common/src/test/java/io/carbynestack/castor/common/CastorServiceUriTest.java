/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common;

import static io.carbynestack.castor.common.CastorServiceUri.INVALID_SERVICE_ADDRESS_EXCEPTION_MSG;
import static io.carbynestack.castor.common.CastorServiceUri.MUST_NOT_BE_EMPTY_EXCEPTION_MSG;
import static io.carbynestack.castor.common.rest.CastorRestApiEndpoints.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carbynestack.castor.common.entities.TupleType;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CastorServiceUriTest {

  @Test
  void givenNullAsServiceAddress_whenCreatingCastorServiceUri_thenThrowIllegalArgumentException() {
    IllegalArgumentException expectedIae =
        assertThrows(IllegalArgumentException.class, () -> new CastorServiceUri(null));
    assertEquals(MUST_NOT_BE_EMPTY_EXCEPTION_MSG, expectedIae.getMessage());
  }

  @Test
  void givenNoSchemeDefined_whenCreatingCastorServiceUri_thenThrowIllegalArgumentException() {
    IllegalArgumentException iae =
        assertThrows(IllegalArgumentException.class, () -> new CastorServiceUri("localhost:8080"));
    assertEquals(INVALID_SERVICE_ADDRESS_EXCEPTION_MSG, iae.getMessage());
  }

  @Test
  void givenInvalidUriString_whenCreatingCastorServiceUri_thenThrowIllegalArgumentException() {
    IllegalArgumentException iae =
        assertThrows(IllegalArgumentException.class, () -> new CastorServiceUri("invalidUri"));
    assertEquals(INVALID_SERVICE_ADDRESS_EXCEPTION_MSG, iae.getMessage());
  }

  @Test
  void
      givenHttpsUriStringWithDomain_whenCreatingCastorServiceUri_thenCreateExpectedCastorServiceUri() {
    CastorServiceUri serviceUri = new CastorServiceUri("https://castor.carbynestack.io:8080");
    assertEquals("https://castor.carbynestack.io:8080", serviceUri.getRestServiceUri().toString());
    assertEquals("castor.carbynestack.io", serviceUri.getRestServiceUri().getHost());
    assertEquals(8080, serviceUri.getRestServiceUri().getPort());
    assertEquals(
        "wss://castor.carbynestack.io:8080/intra-vcp/ws",
        serviceUri.getIntraVcpWsServiceUri().toString());
    assertEquals("castor.carbynestack.io", serviceUri.getIntraVcpWsServiceUri().getHost());
    assertEquals(8080, serviceUri.getIntraVcpWsServiceUri().getPort());
    assertEquals("/intra-vcp/ws", serviceUri.getIntraVcpWsServiceUri().getPath());
  }

  @Test
  void
      givenWsUriStringWithDomain_whenCreatingCastorServiceUri_thenCreateExpectedCastorServiceUri() {
    CastorServiceUri serviceUri = new CastorServiceUri("ws://castor.carbynestack.io:8080");
    assertEquals("http://castor.carbynestack.io:8080", serviceUri.getRestServiceUri().toString());
    assertEquals("castor.carbynestack.io", serviceUri.getRestServiceUri().getHost());
    assertEquals(8080, serviceUri.getRestServiceUri().getPort());
    assertEquals(
        "ws://castor.carbynestack.io:8080/intra-vcp/ws",
        serviceUri.getIntraVcpWsServiceUri().toString());
    assertEquals("castor.carbynestack.io", serviceUri.getIntraVcpWsServiceUri().getHost());
    assertEquals(8080, serviceUri.getIntraVcpWsServiceUri().getPort());
    assertEquals("/intra-vcp/ws", serviceUri.getIntraVcpWsServiceUri().getPath());
  }

  @Test
  void
      givenUriStringWithTrailingSlash_whenCreateCastorServiceUri_thenReturnExpectedCastorServiceUri() {
    CastorServiceUri aUri = new CastorServiceUri("https://castor.carbynestack.io:8081/");
    URI inputMaskUri = aUri.getIntraVcpTelemetryUri();
    assertEquals(
        String.format(
            "https://castor.carbynestack.io:8081%s",
            INTRA_VCP_OPERATIONS_SEGMENT + TELEMETRY_ENDPOINT),
        inputMaskUri.toString());
    assertEquals("castor.carbynestack.io", inputMaskUri.getHost());
    assertEquals(8081, inputMaskUri.getPort());
    assertEquals(INTRA_VCP_OPERATIONS_SEGMENT + TELEMETRY_ENDPOINT, inputMaskUri.getPath());
  }

  @Test
  void givenCastorServiceUri_whenGetActivateTupleChunkUri_thenReturnExpectedUri() {
    CastorServiceUri serviceUri = new CastorServiceUri("https://castor.carbynestack.io:8081");
    UUID chunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    String expectedPath =
        INTRA_VCP_OPERATIONS_SEGMENT + ACTIVATE_TUPLE_CHUNK_ENDPOINT + "/" + chunkId;
    URI actualTupleActivationUri = serviceUri.getIntraVcpActivateTupleChunkResourceUri(chunkId);
    assertEquals(
        String.format("https://castor.carbynestack.io:8081%s", expectedPath),
        actualTupleActivationUri.toString());
    assertEquals(expectedPath, actualTupleActivationUri.getPath());
  }

  @Test
  void givenCastorServiceUri_whenGetRequestTupleUri_thenReturnExpectedUri() {
    CastorServiceUri serviceUri = new CastorServiceUri("https://castor.carbynestack.io:8081");
    UUID requestId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    int count = 3;
    URI actualRequestTuplesUri =
        serviceUri.getIntraVcpRequestTuplesUri(requestId, tupleType, count);
    assertEquals(INTRA_VCP_OPERATIONS_SEGMENT + TUPLES_ENDPOINT, actualRequestTuplesUri.getPath());
    assertThat(actualRequestTuplesUri.getQuery())
        .contains(
            DOWNLOAD_COUNT_PARAMETER + "=" + count,
            DOWNLOAD_REQUEST_ID_PARAMETER + "=" + requestId,
            DOWNLOAD_TUPLE_TYPE_PARAMETER + "=" + tupleType.name());
  }

  @Test
  void givenCastorServiceUri_whenGetRequestTelemetryUri_thenReturnExpectedUri() {
    CastorServiceUri serviceUri = new CastorServiceUri("https://castor.carbynestack.io:8081");
    long interval = 5000L;
    URI actualRequestTelemetryUri = serviceUri.getRequestTelemetryUri(interval);
    assertEquals(
        INTRA_VCP_OPERATIONS_SEGMENT + TELEMETRY_ENDPOINT, actualRequestTelemetryUri.getPath());
    assertThat(actualRequestTelemetryUri.getQuery()).contains(TELEMETRY_INTERVAL + "=" + interval);
  }

  @Test
  void givenValidPathSegments_whenBuildingResourceUri_thenReturnExpectedUri()
      throws URISyntaxException {
    String baseUri = "https://castor.carbynestack.io/Castor";
    String pathVariable = "1234";
    CastorServiceUri CastorServiceUri = new CastorServiceUri(baseUri);
    assertEquals(
        String.format("%s/%s", baseUri, pathVariable),
        CastorServiceUri.attachPathParameter(new URI(baseUri), pathVariable).toString());
  }
}
