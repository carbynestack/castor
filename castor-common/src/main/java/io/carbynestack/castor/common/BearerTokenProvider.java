/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.common;

import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/** Functional interface for providing a bearer token based on an {@link CastorServiceUri} value. */
@Value
@Builder
public class BearerTokenProvider implements Function<CastorServiceUri, String> {
  @Singular Map<CastorServiceUri, String> bearerTokens;

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
}
