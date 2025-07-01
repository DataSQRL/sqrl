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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtContainerIT extends SqrlContainerTestBase {

  private static final Logger logger = LoggerFactory.getLogger(JwtContainerIT.class);
  private static final String JWT_SECRET = "testSecretThatIsAtLeast256BitsLong32Chars";
  private static final String JWT_ISSUER = "my-test-issuer";
  private static final String JWT_AUDIENCE = "my-test-audience";

  @AfterEach
  void tearDown() {
    cleanupContainers();
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStarted_thenUnauthorizedRequestsReturn401() {
    var testDir = itPath("jwt-authorized");

    logger.info("Running JWT container test (unauthorized)");

    compileSqrlScript("jwt-authorized.sqrl", testDir);

    startGraphQLServer(testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    logger.info("JWT unauthorized test completed successfully");
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStartedWithValidJwt_thenAuthorizedRequestsSucceed() {
    var testDir = itPath("jwt-authorized");

    logger.info("Running JWT container test (authorized)");

    compileSqrlScript("jwt-authorized.sqrl", testDir);

    startGraphQLServer(testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}", generateJwtToken());

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = objectMapper.readTree(responseBody);

    assertThat(jsonResponse.has("data")).isTrue();
    assertThat(jsonResponse.get("data").has("__typename")).isTrue();
    assertThat(jsonResponse.get("data").get("__typename").asText()).isEqualTo("Query");

    logger.info("JWT authorized test completed successfully");
  }

  private String generateJwtToken() {
    var key = Keys.hmacShaKeyFor(JWT_SECRET.getBytes(StandardCharsets.UTF_8));
    var now = Instant.now();
    var expiration = now.plusSeconds(20);

    return Jwts.builder()
        .setIssuer(JWT_ISSUER)
        .setAudience(JWT_AUDIENCE)
        .setIssuedAt(Date.from(now))
        .setExpiration(Date.from(expiration))
        .signWith(key, SignatureAlgorithm.HS256)
        .compact();
  }
}
