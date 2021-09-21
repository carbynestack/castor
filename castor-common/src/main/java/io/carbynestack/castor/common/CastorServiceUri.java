/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common;

import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.common.rest.CastorRestApiEndpoints;
import io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints;
import io.vavr.control.Try;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

/**
 * A class, that manages an Castor Service's URI and provides correct paths for all endpoints. Uses
 * {@link URI} internally.
 */
@Data
@Setter(AccessLevel.NONE)
public class CastorServiceUri {
  public static final String INVALID_SERVICE_ADDRESS_EXCEPTION_MSG =
      "Invalid service address.\n"
          + "Address must match the following examples:\n"
          + "\t1. http://server:port\n"
          + "\t2. https://server:port/path\n"
          + "\t3. ws://server\n"
          + "\t4. wss://server/path\n";

  private static final String URI_HTTP_SCHEME = "http";
  private static final String URI_HTTPS_SCHEME = "https";
  private static final String URI_WEBSOCKET_SCHEME = "ws";
  private static final String URI_SECURE_WEBSOCKET_SCHEME = "wss";
  public static final String MUST_NOT_BE_EMPTY_EXCEPTION_MSG = "serviceAddress must not be empty!";

  private final URI restServiceUri;
  private final URI intraVcpTelemetryUri;
  private final URI intraVcpTuplesUri;

  @Getter(AccessLevel.PRIVATE)
  private final URI intraVcpActivateTupleChunksUri;

  private final URI interVcpReservationUri;
  private final URI intraVcpWsServiceUri;

  /**
   * Constructs a new <code>CastorServiceUri</code> with the given address.
   *
   * @param serviceAddress The base URI of an Castor Service. Will be parsed to <code>java.net.URI
   *     </code>. Must match the HTTP or WebSocket protocol.
   * @throws IllegalArgumentException if given serviceAddress is null or empty
   * @throws IllegalArgumentException if {@link CastorServiceUri} could not be constructed
   */
  public CastorServiceUri(String serviceAddress) throws IllegalArgumentException {
    if (StringUtils.isEmpty(serviceAddress)) {
      throw new IllegalArgumentException(MUST_NOT_BE_EMPTY_EXCEPTION_MSG);
    }
    try {
      URI serviceBaseUri = new URI(serviceAddress);
      if (serviceBaseUri.getHost() == null) {
        throw new IllegalArgumentException(INVALID_SERVICE_ADDRESS_EXCEPTION_MSG);
      }
      boolean isSecureConnection =
          Arrays.asList(URI_HTTPS_SCHEME, URI_SECURE_WEBSOCKET_SCHEME)
              .contains(serviceBaseUri.getScheme());
      URIBuilder uriBuilder = new URIBuilder(serviceBaseUri);
      uriBuilder.setScheme(isSecureConnection ? URI_HTTPS_SCHEME : URI_HTTP_SCHEME);
      this.restServiceUri = uriBuilder.build();
      uriBuilder.setScheme(isSecureConnection ? URI_SECURE_WEBSOCKET_SCHEME : URI_WEBSOCKET_SCHEME);
      uriBuilder.setPath(
          serviceBaseUri.getPath() == null
              ? CastorWebSocketApiEndpoints.WEBSOCKET_ENDPOINT
              : String.format(
                  "%s%s",
                  serviceBaseUri.getPath(), CastorWebSocketApiEndpoints.WEBSOCKET_ENDPOINT));
      this.intraVcpWsServiceUri = uriBuilder.build();
      this.intraVcpTuplesUri =
          composeUri(
              restServiceUri,
              CastorRestApiEndpoints.INTRA_VCP_OPERATIONS_SEGMENT
                  + CastorRestApiEndpoints.TUPLES_ENDPOINT);
      this.intraVcpActivateTupleChunksUri =
          composeUri(
              restServiceUri,
              CastorRestApiEndpoints.INTRA_VCP_OPERATIONS_SEGMENT
                  + CastorRestApiEndpoints.ACTIVATE_TUPLE_CHUNK_ENDPOINT);
      this.intraVcpTelemetryUri =
          composeUri(
              restServiceUri,
              CastorRestApiEndpoints.INTRA_VCP_OPERATIONS_SEGMENT
                  + CastorRestApiEndpoints.TELEMETRY_ENDPOINT);
      this.interVcpReservationUri =
          composeUri(
              restServiceUri,
              CastorRestApiEndpoints.INTER_VCP_OPERATIONS_SEGMENT
                  + CastorRestApiEndpoints.RESERVATION_ENDPOINT);

    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to construct CastorServiceUri", e);
    }
  }

  private URI composeUri(URI inputUri, String path) throws URISyntaxException {
    String baseUri = inputUri.toString();
    baseUri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
    return new URI(String.format("%s%s", baseUri, path));
  }

  /**
   * Gets the URI to update a specified reservation.
   *
   * @param reservationId The id of the reservation to update
   * @return Reservation update URI
   */
  public URI getInterVcpUpdateReservationUri(String reservationId) {
    return attachPathParameter(getInterVcpReservationUri(), reservationId);
  }

  /**
   * Gets the activation URI for a specific {@link TupleChunk} resource.
   *
   * @param chunkId The id of the {@link TupleChunk} to activate
   * @return Tuple chunk activation URI for the given resource
   */
  public URI getIntraVcpActivateTupleChunkResourceUri(UUID chunkId) {
    return attachPathParameter(getIntraVcpActivateTupleChunksUri(), chunkId.toString());
  }

  /**
   * Composes the tuple request URI for the given parameters.
   *
   * @param requestId id to identify the request across all parties in the Carbyne Stack MPC cluster
   * @param tupleType the type of tuples requested
   * @param count the number of tuples requested from the service
   * @return the composed {@link URI}
   * @throws CastorClientException if composing the URI failed
   */
  public URI getIntraVcpRequestTuplesUri(UUID requestId, TupleType tupleType, long count) {
    List<NameValuePair> requestParams =
        Arrays.asList(
            new BasicNameValuePair(
                CastorRestApiEndpoints.DOWNLOAD_COUNT_PARAMETER, String.valueOf(count)),
            new BasicNameValuePair(
                CastorRestApiEndpoints.DOWNLOAD_REQUEST_ID_PARAMETER, requestId.toString()),
            new BasicNameValuePair(
                CastorRestApiEndpoints.DOWNLOAD_TUPLE_TYPE_PARAMETER, tupleType.name()));
    try {
      return new URIBuilder(intraVcpTuplesUri).addParameters(requestParams).build();
    } catch (URISyntaxException e) {
      throw new CastorClientException("Failed to construct tuple request URI.", e);
    }
  }

  /**
   * Composes the telemetry request URI for the given parameter.
   *
   * @param interval the interval as basis for the consumption rate evaluation
   * @return the composed {@link URI}
   * @throws CastorClientException if composing the URI failed
   */
  public URI getRequestTelemetryUri(long interval) {
    try {
      return new URIBuilder(intraVcpTelemetryUri)
          .addParameter(CastorRestApiEndpoints.TELEMETRY_INTERVAL, String.valueOf(interval))
          .build();
    } catch (URISyntaxException e) {
      throw new CastorClientException("Failed to construct tuple request URI.", e);
    }
  }

  URI attachPathParameter(URI uri, String param) {
    URIBuilder uriBuilder = new URIBuilder(uri);
    String path = uriBuilder.getPath();
    path =
        String.format(
            "%s%s%s",
            path == null || path.length() == 0 ? "" : path,
            path != null && path.length() > 0 && path.lastIndexOf('/') == path.length() ? "" : "/",
            param);
    uriBuilder.setPath(path);
    return Try.of(uriBuilder::build)
        .getOrElseThrow(
            throwable ->
                new CastorServiceException(
                    String.format(
                        "Failed to compose CastorServiceUri for endpoint \"%s\" and param: %s",
                        uri, param),
                    throwable));
  }
}
