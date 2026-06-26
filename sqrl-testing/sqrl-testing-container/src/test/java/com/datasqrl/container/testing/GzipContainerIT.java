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
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import io.jsonwebtoken.Jwts;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Verifies gzip works both ways (compressed request bodies are decompressed, responses are gzip
 * compressed when the client accepts it) on REST mutation and GraphQL endpoints, with and without
 * JWT auth. Regression test for #2168, where a gzip request body reached the JSON parser still
 * compressed and surfaced as a misleading "JWT auth failed" error.
 */
@Slf4j
public class GzipContainerIT {

  // sensors-mutation: no auth, exposes REST mutation /v1/rest/mutations/SensorReading + GraphQL
  @RegisterExtension
  static SqrlContainerExtension noAuth = new SqrlContainerExtension("sensors-mutation");

  // jwt-authorized package.json: JWT enabled, exposes REST mutation AuthInputData + GraphQL
  @RegisterExtension
  static SqrlContainerExtension jwt = new SqrlContainerExtension("jwt-authorized");

  private static final String TYPENAME_QUERY = "{\"query\":\"query { __typename }\"}";

  @Test
  @SneakyThrows
  void givenNoAuth_whenGzipBothWays_thenRequestDecodedAndResponseCompressed() {
    noAuth.compileAndStartServer();

    assertSoftly(
        softly -> {
          // gzip request body -> GraphQL endpoint
          try (var response = postGzip(noAuth.getGraphQLEndpoint(), TYPENAME_QUERY, null)) {
            softly
                .assertThat(response.getStatusLine().getStatusCode())
                .as("gzip GraphQL request decoded")
                .isEqualTo(200);
          } catch (Exception e) {
            softly.fail("gzip GraphQL request failed", e);
          }

          // gzip request body -> REST mutation endpoint
          var mutationBody = "{\"event\":[{\"sensorid\":1,\"temperature\":43.5}]}";
          try (var response =
              postGzip(
                  noAuth.getBaseUrl() + "/v1/rest/mutations/SensorReading", mutationBody, null)) {
            softly
                .assertThat(response.getStatusLine().getStatusCode())
                .as("gzip REST mutation request decoded")
                .isEqualTo(200);
          } catch (Exception e) {
            softly.fail("gzip REST mutation request failed", e);
          }

          // response gzip compression
          softly
              .assertThat(responseContentEncoding(noAuth.getGraphQLEndpoint(), null))
              .as("response gzip compressed")
              .isEqualTo("gzip");
        });
  }

  @Test
  @SneakyThrows
  void givenJwt_whenGzipBothWays_thenRequestDecodedAndResponseCompressed() {
    jwt.compileAndStartServer("package.json");
    var token = generateJwtToken();

    assertSoftly(
        softly -> {
          // gzip request body -> GraphQL endpoint, with valid JWT
          try (var response = postGzip(jwt.getGraphQLEndpoint(), TYPENAME_QUERY, token)) {
            softly
                .assertThat(response.getStatusLine().getStatusCode())
                .as("gzip GraphQL request decoded with JWT")
                .isEqualTo(200);
          } catch (Exception e) {
            softly.fail("gzip GraphQL request with JWT failed", e);
          }

          // gzip request body -> REST mutation endpoint, with valid JWT (val comes from auth.val).
          // The REST endpoint flattens AuthInputData's input to a required "message" parameter.
          var mutationBody = "{\"message\":\"hello\"}";
          try (var response =
              postGzip(
                  jwt.getBaseUrl() + "/v1/rest/mutations/AuthInputData", mutationBody, token)) {
            softly
                .assertThat(response.getStatusLine().getStatusCode())
                .as("gzip REST mutation request decoded with JWT")
                .isEqualTo(200);
          } catch (Exception e) {
            softly.fail("gzip REST mutation request with JWT failed", e);
          }

          // response gzip compression, with valid JWT
          softly
              .assertThat(responseContentEncoding(jwt.getGraphQLEndpoint(), token))
              .as("response gzip compressed with JWT")
              .isEqualTo("gzip");
        });
  }

  /** POSTs a gzip-compressed body with Content-Encoding: gzip. */
  @SneakyThrows
  private static CloseableHttpResponse postGzip(String url, String json, String jwtToken) {
    var request = new HttpPost(url);
    request.setEntity(new ByteArrayEntity(gzip(json), ContentType.APPLICATION_JSON));
    request.setHeader("Content-Encoding", "gzip");
    if (jwtToken != null) {
      request.setHeader("Authorization", "Bearer " + jwtToken);
    }
    return noAuth.getHttpClient().execute(request);
  }

  /**
   * POSTs a plain body advertising Accept-Encoding: gzip and returns the response Content-Encoding.
   * Uses a client with content-compression disabled so the gzip header is not stripped by
   * transparent decompression.
   */
  @SneakyThrows
  private static String responseContentEncoding(String url, String jwtToken) {
    try (var client = HttpClients.custom().disableContentCompression().build()) {
      var request = new HttpPost(url);
      request.setEntity(new StringEntity(TYPENAME_QUERY, ContentType.APPLICATION_JSON));
      request.setHeader("Accept-Encoding", "gzip");
      if (jwtToken != null) {
        request.setHeader("Authorization", "Bearer " + jwtToken);
      }
      try (var response = client.execute(request)) {
        EntityUtils.consumeQuietly(response.getEntity());
        var header = response.getFirstHeader("Content-Encoding");
        return header == null ? null : header.getValue();
      }
    }
  }

  @SneakyThrows
  private static byte[] gzip(String content) {
    var bos = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(bos)) {
      gzip.write(content.getBytes(UTF_8));
    }
    return bos.toByteArray();
  }

  private String generateJwtToken() {
    var now = Instant.now();
    var expiration = now.plus(1, ChronoUnit.HOURS);
    return Jwts.builder()
        .issuer("my-test-issuer")
        .audience()
        .add("my-test-audience")
        .and()
        .issuedAt(Date.from(now))
        .expiration(Date.from(expiration))
        .claim("val", 1)
        .claim("values", List.of(1, 2, 3))
        .signWith(
            new SecretKeySpec(
                "testSecretThatIsAtLeast256BitsLong32Chars".getBytes(UTF_8), "HmacSHA256"))
        .compact();
  }
}
