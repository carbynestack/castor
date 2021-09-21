/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.config;

import io.carbynestack.castor.common.websocket.CastorWebSocketApiEndpoints;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.SessionRepository;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

  private static final String ALLOWED_ORIGINS = "*";

  private final CastorServiceProperties castorServiceProperties;

  public WebSocketConfig(CastorServiceProperties castorServiceProperties) {
    this.castorServiceProperties = castorServiceProperties;
  }

  @Override
  public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
    stompEndpointRegistry
        .addEndpoint(CastorWebSocketApiEndpoints.WEBSOCKET_ENDPOINT)
        .setAllowedOrigins(ALLOWED_ORIGINS);
  }

  @Override
  public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
    registration.setMessageSizeLimit(castorServiceProperties.getMessageBuffer());
  }

  @Override
  public void configureMessageBroker(MessageBrokerRegistry brokerRegistry) {
    long[] heartbeat = {
      castorServiceProperties.getServerHeartbeat(), castorServiceProperties.getClientHeartbeat()
    };
    brokerRegistry
        .setApplicationDestinationPrefixes(CastorWebSocketApiEndpoints.APPLICATION_PREFIX)
        .enableSimpleBroker(CastorWebSocketApiEndpoints.BROKER_PREFIX)
        .setTaskScheduler(createTaskScheduler())
        .setHeartbeatValue(heartbeat);
  }

  private TaskScheduler createTaskScheduler() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.initialize();
    return taskScheduler;
  }

  @Bean
  public SessionRepository sessionRepository() {
    MapSessionRepository sessionRepository = new MapSessionRepository();
    sessionRepository.setDefaultMaxInactiveInterval(-1);
    return sessionRepository;
  }

  @Bean
  public ServletServerContainerFactoryBean createWebSocketContainer() {
    ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
    container.setMaxBinaryMessageBufferSize(castorServiceProperties.getMessageBuffer());
    return container;
  }
}
