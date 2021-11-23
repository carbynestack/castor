/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

/** Functional interface for providing a bearer token based on an {@link CastorServiceUri} value. */
@Value
public class BearerTokenProvider implements Function<CastorServiceUri, String> {
  Map<CastorServiceUri, String> bearerTokens;

  private BearerTokenProvider(Map<CastorServiceUri, String> bearerTokens) {
    this.bearerTokens = bearerTokens;
  }

  /**
   * Returns the bearer token for the given {@link CastorServiceUri} or <i>null</i> of not token is
   * defined for the given uri.
   *
   * @return the bearer token for the given {@link CastorServiceUri} or <i>null</i> of not token is
   *     defined for the * given uri.
   */
  @Override
  public String apply(CastorServiceUri serviceUri) {
    return bearerTokens.get(serviceUri);
  }

  public static BearerTokenProviderBuilder builder() {
    return new BearerTokenProviderBuilder();
  }

  public static class BearerTokenProviderBuilder {
    private final Map<CastorServiceUri, String> bearerTokens = new HashMap<>();

    private BearerTokenProviderBuilder() {}

    public BearerTokenProviderBuilder bearerToken(CastorServiceUri castorServiceUri, String token) {
      this.bearerTokens.put(castorServiceUri, token);
      return this;
    }

    @Override
    public String toString() {
      return "BearerTokenProviderBuilder{" + "bearerTokens=" + bearerTokens + '}';
    }

    public BearerTokenProvider build() {
      return new BearerTokenProvider(bearerTokens);
    }
  }
}
