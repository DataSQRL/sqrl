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
package com.datasqrl.server.auth;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public record TestJwtKeys(JsonObject publicJwk, JWTAuth signer, String algorithm) {

  public static TestJwtKeys hmac(Vertx vertx, String secret) {
    return hmac(vertx, secret, "HS256");
  }

  /** An {@code oct} key without {@code alg}, and a signer that signs with the given algorithm. */
  public static TestJwtKeys hmac(Vertx vertx, String secret, String algorithm) {
    var jwk =
        new JsonObject()
            .put("kty", "oct")
            .put("k", base64Url(secret.getBytes(StandardCharsets.UTF_8)));
    var signer =
        JWTAuth.create(vertx, new JWTAuthOptions().addJwk(jwk.copy().put("alg", algorithm)));
    return new TestJwtKeys(jwk, signer, algorithm);
  }

  public static TestJwtKeys rsa(Vertx vertx) {
    return rsa(vertx, "RS256");
  }

  /** An RSA key without {@code alg}, and a signer that signs with the given algorithm. */
  @SneakyThrows
  public static TestJwtKeys rsa(Vertx vertx, String algorithm) {
    var generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    var keyPair = generator.generateKeyPair();
    var publicKey = (RSAPublicKey) keyPair.getPublic();
    var jwk =
        new JsonObject()
            .put("kty", "RSA")
            .put("n", base64Url(unsigned(publicKey.getModulus())))
            .put("e", base64Url(unsigned(publicKey.getPublicExponent())));
    var pem =
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder().encodeToString(keyPair.getPrivate().getEncoded())
            + "\n-----END PRIVATE KEY-----";
    var signer =
        JWTAuth.create(
            vertx,
            new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions().setAlgorithm(algorithm).setBuffer(pem)));
    return new TestJwtKeys(jwk, signer, algorithm);
  }

  public static Map<String, Object> claims(String issuer, String audience) {
    return claims(issuer, audience, Instant.now().plusSeconds(300));
  }

  public static Map<String, Object> claims(String issuer, String audience, Instant expiresAt) {
    var claims = new HashMap<String, Object>();
    if (issuer != null) {
      claims.put("iss", issuer);
    }
    if (audience != null) {
      claims.put("aud", List.of(audience));
    }
    claims.put("exp", expiresAt.getEpochSecond());
    return claims;
  }

  public String token(Map<String, Object> claims) {
    return signer.generateToken(
        new JsonObject(claims), new JWTOptions().setAlgorithm(algorithm).setNoTimestamp(true));
  }

  private static byte[] unsigned(BigInteger value) {
    var bytes = value.toByteArray();
    return bytes[0] == 0 ? Arrays.copyOfRange(bytes, 1, bytes.length) : bytes;
  }

  private static String base64Url(byte[] bytes) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }
}
