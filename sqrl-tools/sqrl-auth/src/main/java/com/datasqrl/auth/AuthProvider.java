package com.datasqrl.auth;

import static com.datasqrl.auth.AuthUtils.*;

import com.datasqrl.util.FileUtil;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
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

  private static final Path DATASQRL_CONFIG_DIRECTORY = FileUtil.getUserRoot().resolve(".datasqrl");
  private static final Path REFRESH_TOKEN_PATH = DATASQRL_CONFIG_DIRECTORY.resolve("auth");

  private static final long TIMEOUT_IN_SECONDS = 60;

  private final String state = getRandomString(16);
  private final String codeVerifier = getRandomString(43);
  private final String codeChallenge = generateCodeChallenge(codeVerifier);

  private final HttpClient client;
  private String accessToken;

  public AuthProvider(HttpClient client) {
    this.client = client;
  }

  @SneakyThrows
  public String loginToRepository() {
    return obtainRefreshToken()
        .flatMap(this::refreshAccessToken)
        .orElseGet(this::browserFlow);
  }

  private Optional<String> obtainRefreshToken() throws IOException {
    return Files.isRegularFile(REFRESH_TOKEN_PATH) ?
        Optional.of(Files.readString(REFRESH_TOKEN_PATH)) :
        Optional.empty();
  }

  private Optional<String> refreshAccessToken(String refreshToken) {
    Map<Object, Object> data = prepareData(refreshToken);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(TOKEN_ENDPOINT))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(ofFormData(data))
        .build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        JsonObject jsonResponse = new JsonObject(response.body());
        String newAccessToken = jsonResponse.getString("access_token");
        // Optionally save new refresh token if provided
        String newRefreshToken = jsonResponse.containsKey("refresh_token") ?
            jsonResponse.getString("refresh_token") : refreshToken;
        Files.write(REFRESH_TOKEN_PATH, newRefreshToken.getBytes(StandardCharsets.UTF_8));
        // Save or handle tokens as needed
        return Optional.of(newAccessToken);
      } else {
        log.warn("Refresh token expired.");
      }
    } catch (IOException | InterruptedException e) {
      log.error("Error during token refresh", e);
    }
    return Optional.empty();
  }

  private Map<Object, Object> prepareData(String refreshToken) {
    Map<Object, Object> data = new HashMap<>();
    data.put("grant_type", "refresh_token");
    data.put("refresh_token", refreshToken);
    data.put("client_id", CLIENT_ID);
    return data;
  }

  @SneakyThrows
  private String browserFlow() {
    CompletableFuture<JsonObject> tokenFuture = new CompletableFuture<>();

    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(
        new OAuthCallbackVerticle(
            code -> exchangeCodeForToken(code, tokenFuture)));

    openAuthWindow();

    try {
      JsonObject authToken = tokenFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
      accessToken = authToken.getString("access_token");
      String refreshToken = authToken.getString("refresh_token");

      Files.createDirectories(DATASQRL_CONFIG_DIRECTORY);

      Files.write(REFRESH_TOKEN_PATH, refreshToken.getBytes(StandardCharsets.UTF_8));

      return accessToken;
    } catch (TimeoutException | ExecutionException | InterruptedException e) {
      throw new RuntimeException("Exception while getting access token.", e);
    } finally {
      vertx.close();
    }
  }

  private static HttpRequest.BodyPublisher ofFormData(Map<Object, Object> data) {
    var builder = new StringBuilder();
    for (Map.Entry<Object, Object> entry : data.entrySet()) {
      if (builder.length() > 0) {
        builder.append("&");
      }
      builder.append(URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8));
      builder.append("=");
      builder.append(URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8));
    }
    return HttpRequest.BodyPublishers.ofString(builder.toString());
  }

  @SneakyThrows
  public String isAuthenticated() {
    if (accessToken != null) {
      return accessToken;
    }

    return null;
  }

  private void exchangeCodeForToken(String code, CompletableFuture<JsonObject> tokenFuture) {
    JsonObject payload = new JsonObject().put("grant_type", "authorization_code")
        .put("client_id", CLIENT_ID).put("code_verifier", codeVerifier).put("code", code)
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
    Map<String, String> parameters = Map.of("response_type", "code",
        "client_id", CLIENT_ID,
        "redirect_uri", URLEncoder.encode(REDIRECT_URI, StandardCharsets.UTF_8), "scope",
        "offline_access", "state", state, "audience",
        "https://sqrl-repository-frontend-git-staging-datasqrl.vercel.app/api/client",
        "code_challenge", codeChallenge, "code_challenge_method", "S256");

    String paramString = parameters.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("&"));

    String authUrl = "https://" + AUTH0_DOMAIN + "/authorize?" + paramString;

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
