/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.testconfig;

import org.testcontainers.containers.PostgreSQLContainer;

public class ReusablePostgreSQLContainer extends PostgreSQLContainer<ReusablePostgreSQLContainer> {
  private static final String IMAGE_VERSION = "postgres:11.1";
  private static ReusablePostgreSQLContainer container;

  private ReusablePostgreSQLContainer() {
    super(IMAGE_VERSION);
  }

  public static ReusablePostgreSQLContainer getInstance() {
    if (container == null) {
      container = new ReusablePostgreSQLContainer();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();
    System.setProperty("POSTGRESQL_URL", container.getJdbcUrl());
    System.setProperty("POSTGRESQL_USERNAME", container.getUsername());
    System.setProperty("POSTGRESQL_PASSWORD", container.getPassword());
  }

  @Override
  public void stop() {
    // container should stay alive until JVM shuts it down
  }
}
