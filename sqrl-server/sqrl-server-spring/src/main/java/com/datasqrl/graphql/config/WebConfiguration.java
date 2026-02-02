/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.graphql.config;

import com.datasqrl.graphql.JsonEnvVarDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Web configuration for the Spring Boot GraphQL server. Configures CORS and other web-related
 * settings.
 */
@Slf4j
@Configuration
@EnableWebFlux
@RequiredArgsConstructor
public class WebConfiguration implements WebFluxConfigurer {

  private final ServerConfigProperties config;

  @Override
  public void addCorsMappings(CorsRegistry registry) {
    var corsConfig = config.getCors();
    if (corsConfig == null) {
      log.info("No CORS configuration found, using defaults");
      return;
    }

    var corsRegistration = registry.addMapping("/**");

    if (corsConfig.getAllowedOrigin() != null) {
      corsRegistration.allowedOrigins(corsConfig.getAllowedOrigin());
    }

    if (corsConfig.getAllowedOrigins() != null && !corsConfig.getAllowedOrigins().isEmpty()) {
      corsRegistration.allowedOrigins(corsConfig.getAllowedOrigins().toArray(String[]::new));
    }

    if (corsConfig.getAllowedMethods() != null) {
      corsRegistration.allowedMethods(corsConfig.getAllowedMethods().toArray(String[]::new));
    }

    if (corsConfig.getAllowedHeaders() != null) {
      corsRegistration.allowedHeaders(corsConfig.getAllowedHeaders().toArray(String[]::new));
    }

    if (corsConfig.getExposedHeaders() != null) {
      corsRegistration.exposedHeaders(corsConfig.getExposedHeaders().toArray(String[]::new));
    }

    corsRegistration.allowCredentials(corsConfig.isAllowCredentials());
    corsRegistration.maxAge(corsConfig.getMaxAgeSeconds());

    log.info(
        "CORS configured: origins={}, methods={}",
        corsConfig.getAllowedOrigins(),
        corsConfig.getAllowedMethods());
  }

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    var objectMapper = new ObjectMapper();
    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new Jdk8Module());
    return objectMapper;
  }
}
