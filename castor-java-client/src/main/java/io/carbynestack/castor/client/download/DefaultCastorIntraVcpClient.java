/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import static java.util.Collections.singletonList;

import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.InputMask;
import io.carbynestack.castor.common.entities.TelemetryData;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.httpclient.BearerTokenUtils;
import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.util.List;
import java.util.UUID;
import lombok.Value;
import org.apache.http.Header;

/**
 * The default implementation of a {@link CastorIntraVcpClient}. It can be used to download {@link
 * io.carbynestack.castor.common.entities.MultiplicationTriple}s or {@link InputMask}s from one ore
 * more castor service(s).
 */
@Value
public class DefaultCastorIntraVcpClient implements CastorIntraVcpClient {
  public static final String FAILED_DOWNLOADING_TUPLES_EXCEPTION_MSG =
      "Failed downloading tuples from service %s: %s";
  public static final String FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG =
      "Failed fetching telemetry data from service %s: %s";
  CsHttpClient<String> csHttpClient;
  CastorServiceUri serviceUri;
  Option<BearerTokenProvider> bearerTokenProvider;

  /**
   * Creates a new {@link DefaultCastorIntraVcpClient} with the specified {@link Builder}
   * configuration
   *
   * <p>The client is capable to communicate with the given services using either http or https,
   * according to the scheme defined by the give url. In addition trustworthy SSL certificates can
   * be defined to allow secure communication with services that provide self-signed certificates or
   * ssl certificate validation can be disabled.
   *
   * @param builder An {@link Builder} object containing the client's configuration.
   * @throws CastorClientException if internal {@link CsHttpClient} could not be build
   */
  private DefaultCastorIntraVcpClient(Builder builder) {
    this(
        builder,
        Try.of(
                () -> {
                  try {
                    return CsHttpClient.<String>builder()
                        .withFailureType(String.class)
                        .withoutSslValidation(builder.noSslValidation)
                        .withTrustedCertificates(builder.trustedCertificates)
                        .build();
                  } catch (CsHttpClientException chce) {
                    throw new CastorClientException("Failed to create CsHttpClient.", chce);
                  }
                })
            .get());
  }

  /**
   * Creates a new {@link DefaultCastorIntraVcpClient} with the specified {@link Builder}
   * configuration and a given {@link CsHttpClient}.
   *
   * <p>The client is capable to communicate with the given services using either http or https,
   * according to the scheme defined by the give url. In addition trustworthy SSL certificates can
   * be defined to allow secure communication with services that provide self-signed certificates or
   * ssl certificate validation can be disabled.
   *
   * @param builder An {@link Builder} object containing the client's configuration.
   * @param csHttpClient The {@link CsHttpClient} used for communication with the service.
   */
  DefaultCastorIntraVcpClient(Builder builder, CsHttpClient<String> csHttpClient) {
    this.serviceUri = builder.serviceUris.get(0);
    this.bearerTokenProvider = Option.of(builder.bearerTokenProvider);
    this.csHttpClient = csHttpClient;
  }

  @Override
  public TupleList downloadTupleShares(UUID requestId, TupleType tupleType, long count) {
    try {
      return csHttpClient
          .getForEntity(
              serviceUri.getIntraVcpRequestTuplesUri(requestId, tupleType, count),
              getHeaders(serviceUri),
              TupleList.class)
          .get();
    } catch (CsHttpClientException chce) {
      throw new CastorClientException(
          String.format(
              FAILED_DOWNLOADING_TUPLES_EXCEPTION_MSG,
              serviceUri.getRestServiceUri(),
              chce.getMessage()),
          chce);
    }
  }

  @Override
  public TelemetryData getTelemetryData() {
    try {
      return csHttpClient
          .getForEntity(
              serviceUri.getIntraVcpTelemetryUri(), getHeaders(serviceUri), TelemetryData.class)
          .get();
    } catch (CsHttpClientException chce) {
      throw new CastorClientException(
          String.format(
              FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG,
              serviceUri.getRestServiceUri(),
              chce.getMessage()),
          chce);
    }
  }

  @Override
  public TelemetryData getTelemetryData(long interval) {
    try {
      return csHttpClient
          .getForEntity(
              serviceUri.getRequestTelemetryUri(interval),
              getHeaders(serviceUri),
              TelemetryData.class)
          .get();
    } catch (CsHttpClientException chce) {
      throw new CastorClientException(
          String.format(
              FAILED_FETCHING_TELEMETRY_DATA_EXCEPTION_MSG,
              serviceUri.getRestServiceUri(),
              chce.getMessage()),
          chce);
    }
  }

  private List<Header> getHeaders(CastorServiceUri uri) {
    return bearerTokenProvider
        .map(p -> BearerTokenUtils.createBearerToken(p.apply(uri)))
        .toJavaList();
  }

  /**
   * Create a new {@link DefaultCastorIntraVcpClient.Builder} to easily configure and create a new
   * {@link DefaultCastorIntraVcpClient}.
   *
   * @param serviceAddress Address of the service the new {@link DefaultCastorIntraVcpClient} should
   *     communicate with.
   * @throws IllegalArgumentException if the given service addresses is null, empty, or cannot be
   *     parsed as {@link CastorServiceUri}s
   */
  public static Builder builder(String serviceAddress) {
    return new Builder(serviceAddress);
  }

  /** Builder class to create a new {@link DefaultCastorIntraVcpClient}. */
  public static class Builder extends ClientBuilderSupport<Builder, DefaultCastorIntraVcpClient> {

    /**
     * Create a new {@link DefaultCastorIntraVcpClient.Builder} to easily configure and create a new
     * {@link DefaultCastorIntraVcpClient}.
     *
     * @param serviceAddress Address of the service the new {@link DefaultCastorIntraVcpClient}
     *     should communicate with.
     * @throws IllegalArgumentException if the given service addresses is null, empty, or cannot be
     *     parsed as {@link CastorServiceUri}s
     */
    private Builder(String serviceAddress) {
      super(singletonList(serviceAddress));
    }

    @Override
    protected Builder getThis() {
      return this;
    }

    /**
     * Builds and returns a new {@link DefaultCastorIntraVcpClient} according to the given
     * configuration.
     *
     * @throws CastorClientException If the {@link DefaultCastorIntraVcpClient} could not be
     *     instantiated.
     */
    public DefaultCastorIntraVcpClient build() {
      return new DefaultCastorIntraVcpClient(getThis());
    }
  }
}
