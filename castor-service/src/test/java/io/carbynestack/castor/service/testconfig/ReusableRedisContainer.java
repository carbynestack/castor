/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.testconfig;

import static org.springframework.util.Assert.notNull;

import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

@Slf4j
public class ReusableRedisContainer extends GenericContainer<ReusableRedisContainer> {
  private static final String IMAGE_VERSION = "redis:latest";
  private static final int REDIS_PORT = 6379;
  private static final String REDIS_STARTUP_COMMAND = "redis-server";
  private static final URL REDIS_CONFIG_URL =
      ReusableRedisContainer.class.getClassLoader().getResource("redis.conf");
  private static final String CONFIG_BIND_PATH = "/usr/local/etc/redis/redis.conf";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private static ReusableRedisContainer container;

  @SneakyThrows
  private ReusableRedisContainer() {
    super(IMAGE_VERSION);
    notNull(REDIS_CONFIG_URL, "Redis test container config not found.");
    Path tempRedisConfigPath =
        Files.copy(
            FileSystems.getDefault().getPath(REDIS_CONFIG_URL.getPath()),
            Files.createTempFile("redis", "conf"),
            StandardCopyOption.REPLACE_EXISTING);
    tempRedisConfigPath.toFile().deleteOnExit();
    this.withExposedPorts(REDIS_PORT);
    this.withCommand(REDIS_STARTUP_COMMAND, CONFIG_BIND_PATH);
    this.withFileSystemBind(tempRedisConfigPath.toString(), CONFIG_BIND_PATH, BindMode.READ_ONLY);
    this.setWaitStrategy(
        new AbstractWaitStrategy() {
          @Override
          protected void waitUntilReady() {
            Unreliables.retryUntilSuccess(
                (int) STARTUP_TIMEOUT.getSeconds(),
                TimeUnit.SECONDS,
                () -> {
                  ExecResult execResult = container.execInContainer("redis-cli", "ping");
                  if (execResult.getExitCode() != 0) {
                    throw new RuntimeException(
                        String.format("Redis not yet ready: %s", execResult));
                  }
                  return null;
                });
          }
        });
  }

  public static ReusableRedisContainer getInstance() {
    if (container == null) {
      container = new ReusableRedisContainer();
    }
    return container;
  }

  @Override
  public void start() {
    super.start();
    System.setProperty("REDIS_HOST", container.getContainerIpAddress());
    System.setProperty("REDIS_PORT", container.getMappedPort(REDIS_PORT).toString());
  }

  @Override
  public void stop() {
    // container should stay alive until JVM shuts it down
  }
}
