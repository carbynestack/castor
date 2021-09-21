/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.config;

import io.carbynestack.httpclient.CsHttpClient;
import io.carbynestack.httpclient.CsHttpClientException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestConfig {

  @Bean
  public CsHttpClient<String> getRestTemplate(CastorServiceProperties castorServiceProperties)
      throws CsHttpClientException {
    return CsHttpClient.<String>builder()
        .withFailureType(String.class)
        .withoutSslValidation(castorServiceProperties.isNoSslValidation())
        .withTrustedCertificates(castorServiceProperties.getTrustedCertificates())
        .build();
  }
}
