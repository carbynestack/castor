/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.testconfig;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/**
 * Reusable SeaweedFS container (S3 gateway, no authentication) used as S3-compatible object store
 * in integration tests. Any access/secret key is accepted by the server.
 */
public class ReusableSeaweedFsContainer extends GenericContainer<ReusableSeaweedFsContainer> {
  private static final String IMAGE_VERSION = "chrislusf/seaweedfs:3.93";
  private static final int S3_PORT = 8333;
  private static final String ACCESS_KEY = "test-access-key";
  private static final String SECRET_KEY = "test-secret-key";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private static ReusableSeaweedFsContainer container;

  private ReusableSeaweedFsContainer() {
    super(IMAGE_VERSION);
    this.withExposedPorts(S3_PORT);
    this.withCommand("server", "-s3");
    this.setWaitStrategy(
        new HttpWaitStrategy().forPort(S3_PORT).forPath("/").withStartupTimeout(STARTUP_TIMEOUT));
  }

  public static ReusableSeaweedFsContainer getInstance() {
    if (container == null) {
      container = new ReusableSeaweedFsContainer();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();
    System.setProperty(
        "MINIO_ENDPOINT",
        "http://" + container.getContainerIpAddress() + ":" + container.getMappedPort(S3_PORT));
    System.setProperty("MINIO_ACCESS_KEY", ACCESS_KEY);
    System.setProperty("MINIO_SECRET_KEY", SECRET_KEY);
  }

  @Override
  public void stop() {
    // container should stay alive until JVM shuts it down
  }
}
