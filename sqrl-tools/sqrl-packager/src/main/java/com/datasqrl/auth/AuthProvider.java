package com.datasqrl.auth;

import static com.datasqrl.auth.AuthUtils.AUDIENCE;
import static com.datasqrl.auth.AuthUtils.AUTHORIZE_ENDPOINT;
import static com.datasqrl.auth.AuthUtils.CLIENT_ID;
import static com.datasqrl.auth.AuthUtils.REDIRECT_URI;
import static com.datasqrl.auth.AuthUtils.TOKEN_ENDPOINT;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthProvider {
  private static final long TIMEOUT_IN_SECONDS = 120;
  private final String state = getRandomString(16);
  private final String codeVerifier = getRandomString(43);
  private final String codeChallenge = generateCodeChallenge(codeVerifier);

  private final HttpClient client;
  private final TokenManager tokenManager;

  public AuthProvider() {
    this.client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.ALWAYS)
        .build();
    this.tokenManager = new TokenManager();
  }

  public Optional<String> getAccessToken() {
    return tokenManager.getAccessToken()
        .or(()->tokenManager.getRefreshToken()
            .flatMap(this::refreshAccessToken));
  }

  public String loginToRepository() {
    return this.browserFlow();
  }

  private Optional<String> refreshAccessToken(String refreshToken) {
    JsonObject payload = new JsonObject();
    payload.put("grant_type", "refresh_token");
    payload.put("refresh_token", refreshToken);
    payload.put("client_id", CLIENT_ID);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(TOKEN_ENDPOINT))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      int statusCode = response.statusCode();
      if (statusCode == 200) {
        JsonObject jsonResponse = new JsonObject(response.body());

        String newAccessToken = jsonResponse.getString("access_token");
        tokenManager.setAccessToken(newAccessToken);

        //Support rotating refresh tokens if enabled
        String newRefreshToken = jsonResponse.containsKey("refresh_token") ?
            jsonResponse.getString("refresh_token") : refreshToken;
        tokenManager.setRefreshToken(newRefreshToken);

        return Optional.of(newAccessToken);
      } else if (statusCode == 403 ) {
        log.warn(new JsonObject(response.body()).getString("error_description"));
      } else {
        log.error("Error during token refresh: {}", response.body());
      }
    } catch (IOException | InterruptedException e) {
      log.error("Error during token refresh", e);
    }
    return Optional.empty();
  }

  @SneakyThrows
  private String browserFlow() {
    Vertx vertx = Vertx.vertx();

    try {
      CompletableFuture<JsonObject> tokenFuture = new CompletableFuture<>();

      vertx.deployVerticle(
          new OAuthCallbackVerticle(
              code -> exchangeCodeForToken(code, tokenFuture)));

      openAuthWindow();

      JsonObject authToken = tokenFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
      String accessToken = authToken.getString("access_token");
      tokenManager.setAccessToken(accessToken);
      String refreshToken = authToken.getString("refresh_token");
      tokenManager.setRefreshToken(refreshToken);
      return accessToken;
    } catch (TimeoutException | ExecutionException | InterruptedException e) {
      throw new RuntimeException("Exception while getting access token.", e);
    } finally {
      vertx.close();
    }
  }

  private void exchangeCodeForToken(String code, CompletableFuture<JsonObject> tokenFuture) {
    JsonObject payload = new JsonObject()
        .put("grant_type", "authorization_code")
        .put("client_id", CLIENT_ID)
        .put("code_verifier", codeVerifier)
        .put("code", code)
        .put("redirect_uri", REDIRECT_URI);

    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(TOKEN_ENDPOINT))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString())).build();

    client.sendAsync(request, BodyHandlers.ofString())
        .thenAccept(response -> tokenFuture.complete(new JsonObject(response.body())))
        .exceptionally(throwable -> {
          tokenFuture.completeExceptionally(throwable);
          return null;
        });
  }

  private void openAuthWindow() throws IOException {
    Map<String, String> parameters = Map.of(
      "response_type", "code",
      "client_id", CLIENT_ID,
      "redirect_uri", REDIRECT_URI,
      "scope", "offline_access",
      "state", state,
      "audience", AUDIENCE,
      "code_challenge", codeChallenge,
      "code_challenge_method", "S256");

    String paramString = parameters.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
        .collect(Collectors.joining("&"));

    String authUrl = AUTHORIZE_ENDPOINT + "?" + paramString;

    Runtime.getRuntime().exec("open " + authUrl);
  }

  private String getRandomString(int numBytes) {
    SecureRandom random = new SecureRandom();
    byte[] values = new byte[numBytes];
    random.nextBytes(values);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(values);
  }

  @SneakyThrows
  private String generateCodeChallenge(String codeVerifier) {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] digest = md.digest(codeVerifier.getBytes(StandardCharsets.US_ASCII));
    return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
  }
}
