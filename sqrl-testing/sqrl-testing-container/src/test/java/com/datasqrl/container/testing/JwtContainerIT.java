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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.jsonwebtoken.Jwts;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.Date;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

public class JwtContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "jwt-authorized";
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStarted_thenUnauthorizedRequestsReturn401() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStartedWithValidJwt_thenAuthorizedRequestsSucceed() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}", generateJwtToken());
    validateBasicGraphQLResponse(response);
  }

  @Test
  @SneakyThrows
  void
      givenJwtEnabledScript_whenServerStartedWithMismatchedAlgorithm_thenReturns401WithDetailedError() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response =
        executeGraphQLQuery("{\"query\":\"query { __typename }\"}", generateRS256JwtToken());

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    // Verify the response contains detailed error information in JSON format
    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody).contains("\"error\"");
    assertThat(responseBody).contains("\"JWT auth failed\"");
    assertThat(responseBody).contains("\"cause\"");
    assertThat(responseBody).contains("\"NoSuchKeyIdException: algorithm [RS256]: <null>\"");
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenQueryHasCorruptJwt_thenReturns401WithDetailedError() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}", "dummy-invalid-jwt");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    // Verify the response contains detailed error information in JSON format
    var responseBody = EntityUtils.toString(response.getEntity());
    System.out.println(responseBody);
    assertThat(responseBody).contains("\"error\"");
    assertThat(responseBody).contains("\"JWT auth failed\"");
    assertThat(responseBody).contains("\"cause\"");
    assertThat(responseBody).contains("\"IllegalArgumentException: Invalid format for JWT\"");
  }

  private String generateJwtToken() {
    var now = Instant.now();
    var expiration = now.plusSeconds(20);

    return Jwts.builder()
        .issuer("my-test-issuer")
        .audience()
        .add("my-test-audience")
        .and()
        .issuedAt(Date.from(now))
        .expiration(Date.from(expiration))
        .signWith(
            new SecretKeySpec(
                "testSecretThatIsAtLeast256BitsLong32Chars".getBytes(UTF_8), "HmacSHA256"))
        .compact();
  }

  private String generateRS256JwtToken() {
    var now = Instant.now();
    var expiration = now.plusSeconds(20);

    // Generate an RSA key pair for RS256 algorithm (different from server's expected HS256)
    try {
      var keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048);
      var keyPair = keyPairGenerator.generateKeyPair();

      return Jwts.builder()
          .issuer("my-test-issuer")
          .audience()
          .add("my-test-audience")
          .and()
          .issuedAt(Date.from(now))
          .expiration(Date.from(expiration))
          .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
          .compact();
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate RSA key pair for test", e);
    }
  }
}
