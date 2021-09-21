/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.client.download;

import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import io.carbynestack.httpclient.CsResponseEntity;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.net.URI;
import java.util.List;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Slf4j
public class DefaultCastorInterVcpClient implements CastorInterVcpClient {
  public static final String FAILED_SHARING_RESERVATION_EXCEPTION_MSG =
      "Failed sharing Reservation.";
  public static final String FAILED_UPDATING_RESERVATION_EXCEPTION_MSG =
      "Failed sending reservation update.";
  CsHttpClient<String> csHttpClient;
  List<CastorServiceUri> serviceUris;
  Option<BearerTokenProvider> bearerTokenProvider;

  /**
   * Creates a new {@link DefaultCastorInterVcpClient} with the specified {@link Builder}
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
  private DefaultCastorInterVcpClient(Builder builder) {
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
   * Creates a new {@link DefaultCastorInterVcpClient} with the specified {@link Builder}
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
  DefaultCastorInterVcpClient(Builder builder, CsHttpClient<String> csHttpClient) {
    this.serviceUris = builder.serviceUris;
    this.bearerTokenProvider = Option.of(builder.bearerTokenProvider);
    this.csHttpClient = csHttpClient;
  }

  @Override
  public boolean shareReservation(Reservation reservation) {
    boolean result = true;
    log.debug("Sharing reservation {}", reservation);

    try {
      for (CastorServiceUri serviceUri : serviceUris) {
        log.debug("\twith {}", serviceUri.toString());
        CsResponseEntity<String, String> response =
            csHttpClient.postForEntity(
                serviceUri.getInterVcpReservationUri(), reservation, String.class);
        if (response.isFailure()) {
          log.debug(
              "Slave ({}) returned failure for shared reservation: {}",
              serviceUri,
              response.getError());
          result = false;
        }
      }
      return result;
    } catch (CsHttpClientException chce) {
      throw new CastorClientException(FAILED_SHARING_RESERVATION_EXCEPTION_MSG, chce);
    }
  }

  @Override
  public void updateReservationStatus(String reservationId, ActivationStatus status) {
    try {
      for (CastorServiceUri serviceUri : serviceUris) {
        URI requestUri = serviceUri.getInterVcpUpdateReservationUri(reservationId);
        log.debug("Sending reservation update for reservation #{}: {}", reservationId, status);
        csHttpClient.put(requestUri, status);
      }
    } catch (CsHttpClientException chce) {
      throw new CastorClientException(FAILED_UPDATING_RESERVATION_EXCEPTION_MSG, chce);
    }
  }

  /**
   * Create a new {@link Builder} to easily configure and create a new {@link
   * DefaultCastorInterVcpClient}.
   *
   * @param serviceAddresses Addresses of the service(s) the new {@link DefaultCastorInterVcpClient}
   *     should communicate with.
   * @throws NullPointerException if given serviceAddresses is null
   * @throws IllegalArgumentException if a single service address is null or cannot be parsed as
   *     {@link CastorServiceUri}s
   */
  public static Builder builder(List<String> serviceAddresses) {
    return new Builder(serviceAddresses);
  }

  /** Builder class to create a new {@link DefaultCastorInterVcpClient}. */
  public static class Builder extends ClientBuilderSupport<Builder, DefaultCastorInterVcpClient> {

    /**
     * Create a new {@link Builder} to easily configure and create a new {@link
     * DefaultCastorInterVcpClient}.
     *
     * @param serviceAddresses Addresses of the service(s) the new {@link
     *     DefaultCastorInterVcpClient} should communicate with.
     * @throws NullPointerException if given serviceAddresses is null
     * @throws IllegalArgumentException if a single service address is null or cannot be parsed as
     *     {@link CastorServiceUri}s
     */
    private Builder(List<String> serviceAddresses) {
      super(serviceAddresses);
    }

    @Override
    protected Builder getThis() {
      return this;
    }

    /**
     * Builds and returns a new {@link DefaultCastorInterVcpClient} according to the given
     * configuration.
     *
     * @throws CastorClientException If the {@link CastorInterVcpClient} could not be instantiated.
     */
    public DefaultCastorInterVcpClient build() {
      return new DefaultCastorInterVcpClient(getThis());
    }
  }
}
