/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import io.carbynestack.castor.common.BearerTokenProvider;
import io.carbynestack.castor.common.CastorServiceUri;
import io.carbynestack.httpclient.CsHttpClientException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;

/**
 * An abstract class to provide a base builder for castor clients.
 *
 * <p>This {@link ClientBuilderSupport} will ensure that at least one {@link CastorServiceUri} is
 * defined for {@link #serviceUris}.
 *
 * @param <T> The individual implementation of this builder for an individual client.
 * @param <C> The actual client to be build with this builder.
 */
public abstract class ClientBuilderSupport<T extends ClientBuilderSupport<T, C>, C> {
  public static final String ADDRESSES_MUST_NOT_BE_EMPTY_EXCEPTION_MSG =
      "Declared service URIs must not be empty";
  protected final List<CastorServiceUri> serviceUris;
  protected final List<File> trustedCertificates = new ArrayList<>();
  protected boolean noSslValidation = false;
  protected BearerTokenProvider bearerTokenProvider;

  /**
   * Creates a new {@link ClientBuilderSupport} with the given service addresses.
   *
   * @param serviceAddresses Addresses of the services to communicate with.
   * @throws NullPointerException if given serviceAddresses is null
   * @throws IllegalArgumentException if given serviceAddresses is empty
   * @throws IllegalArgumentException if a single given serviceAddress is null or empty
   * @throws IllegalArgumentException if {@link CastorServiceUri} could be constructed
   */
  protected ClientBuilderSupport(@NonNull List<String> serviceAddresses) {
    if (serviceAddresses.isEmpty()) {
      throw new IllegalArgumentException(ADDRESSES_MUST_NOT_BE_EMPTY_EXCEPTION_MSG);
    }
    this.serviceUris = new ArrayList<>();
    for (String address : serviceAddresses) {
      serviceUris.add(new CastorServiceUri(address));
    }
  }

  protected abstract T getThis();

  /**
   * Disables the SSL certificate validation check.
   *
   * <p><b>WARNING</b><br>
   * Please be aware, that this option leads to insecure web connections and is meant to be used in
   * a local test setup only. Using this option in a productive environment is explicitly <u>not
   * recommended</u>.
   */
  public T withoutSslCertificateValidation() {
    this.noSslValidation = true;
    return getThis();
  }

  /**
   * Adds a certificate (.pem) to the trust store.<br>
   * This allows tls secured communication with services that do not have a certificate issued by an
   * official CA (certificate authority).
   *
   * @param trustedCertificate Public certificate.
   */
  public T withTrustedCertificate(File trustedCertificate) {
    this.trustedCertificates.add(trustedCertificate);
    return getThis();
  }

  /**
   * Sets a provider for getting a backend specific bearer token that is injected as an
   * authorization header to REST HTTP calls emitted by the client.
   *
   * @param bearerTokenProvider {@link BearerTokenProvider} to be injected
   */
  public T withBearerTokenProvider(BearerTokenProvider bearerTokenProvider) {
    this.bearerTokenProvider = bearerTokenProvider;
    return getThis();
  }

  public abstract C build() throws CsHttpClientException;
}
