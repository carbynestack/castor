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

public class ReusableMinioContainer extends GenericContainer<ReusableMinioContainer> {
  private static final String IMAGE_VERSION = "minio/minio:edge";
  private static final int MINIO_PORT = 9000;
  private static final String ENV_KEY_MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
  private static final String ENV_KEY_MINIO_SECRET_KEY = "MINIO_SECRET_KEY";
  private static final String ACCESS_KEY = "test-access-key";
  private static final String SECRET_KEY = "test-secret-key";
  private static final String MINIO_STARTUP_COMMAND = "server";
  private static final String MINIO_STORAGE_DIRECTORY = "/data";
  private static final String HEALTH_ENDPOINT = "/minio/health/ready";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private static ReusableMinioContainer container;

  private ReusableMinioContainer() {
    super(IMAGE_VERSION);
    this.withExposedPorts(MINIO_PORT);
    this.withEnv(ENV_KEY_MINIO_ACCESS_KEY, ACCESS_KEY);
    this.withEnv(ENV_KEY_MINIO_SECRET_KEY, SECRET_KEY);
    this.withCommand(MINIO_STARTUP_COMMAND, MINIO_STORAGE_DIRECTORY);
    this.setWaitStrategy(
        new HttpWaitStrategy()
            .forPort(MINIO_PORT)
            .forPath(HEALTH_ENDPOINT)
            .withStartupTimeout(STARTUP_TIMEOUT));
  }

  public static ReusableMinioContainer getInstance() {
    if (container == null) {
      container = new ReusableMinioContainer();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();
    System.setProperty(
        "MINIO_ENDPOINT",
        "http://" + container.getContainerIpAddress() + ":" + container.getMappedPort(MINIO_PORT));
    System.setProperty("MINIO_ACCESS_KEY", ACCESS_KEY);
    System.setProperty("MINIO_SECRET_KEY", SECRET_KEY);
  }

  @Override
  public void stop() {
    // container should stay alive until JVM shuts it down
  }
}
