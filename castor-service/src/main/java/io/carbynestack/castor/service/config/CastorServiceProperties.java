/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "carbynestack.castor")
@Component
@Data
@Accessors(chain = true)
public class CastorServiceProperties {

  private long serverHeartbeat;
  private long clientHeartbeat;
  private int messageBuffer;
  private boolean master;
  private String masterUri;
  private List<String> slaveUris;
  private boolean noSslValidation = false;
  private List<File> trustedCertificates = new ArrayList<>();
  private int initialFragmentSize;

  private CastorCacheProperties cache;
  private CastorSlaveServiceProperties slave;
}
