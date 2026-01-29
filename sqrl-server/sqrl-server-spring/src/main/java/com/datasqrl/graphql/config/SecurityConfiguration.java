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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Spring Security configuration for the GraphQL server. Configures JWT/OAuth2 authentication when
 * configured.
 */
@Slf4j
@Configuration
@EnableWebFluxSecurity
@RequiredArgsConstructor
public class SecurityConfiguration {

  private final ServerConfigProperties config;

  @Bean
  public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
    http.csrf(ServerHttpSecurity.CsrfSpec::disable);

    http.cors(cors -> {});

    var jwtConfig = config.getJwt();
    var oauthConfig = config.getOauth();

    if (jwtConfig != null && jwtConfig.getJwksUri() != null) {
      log.info("Configuring JWT authentication with JWKS URI: {}", jwtConfig.getJwksUri());
      http.oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtDecoder(jwtDecoder())));

      http.authorizeExchange(
          exchanges ->
              exchanges
                  .pathMatchers("/health/**", "/metrics", "/actuator/**")
                  .permitAll()
                  .pathMatchers("/graphiql/**", "/swagger**", "/**/swagger**")
                  .permitAll()
                  .anyExchange()
                  .authenticated());
    } else if (oauthConfig != null && oauthConfig.getSite() != null) {
      log.info("Configuring OAuth2 authentication with site: {}", oauthConfig.getSite());
      http.oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtDecoder(oauthJwtDecoder())));

      http.authorizeExchange(
          exchanges ->
              exchanges
                  .pathMatchers("/health/**", "/metrics", "/actuator/**")
                  .permitAll()
                  .pathMatchers("/graphiql/**", "/swagger**", "/**/swagger**")
                  .permitAll()
                  .anyExchange()
                  .authenticated());
    } else {
      log.info("No authentication configured, allowing all requests");
      http.authorizeExchange(exchanges -> exchanges.anyExchange().permitAll());
    }

    return http.build();
  }

  @Bean
  public ReactiveJwtDecoder jwtDecoder() {
    var jwtConfig = config.getJwt();
    if (jwtConfig != null && jwtConfig.getJwksUri() != null) {
      return ReactiveJwtDecoders.fromIssuerLocation(jwtConfig.getIssuer());
    }
    return null;
  }

  private ReactiveJwtDecoder oauthJwtDecoder() {
    var oauthConfig = config.getOauth();
    if (oauthConfig != null && oauthConfig.getSite() != null) {
      var jwksUri =
          oauthConfig.getSite()
              + (oauthConfig.getJwksPath() != null
                  ? oauthConfig.getJwksPath()
                  : "/.well-known/jwks.json");
      return ReactiveJwtDecoders.fromIssuerLocation(oauthConfig.getSite());
    }
    return null;
  }
}
