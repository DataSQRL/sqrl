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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class AuthProvider {

  private static final Path DATASQRL_CONFIG_DIRECTORY = FileUtil.getUserRoot().resolve(".datasqrl");
  private static final Path AUTH_TOKEN_PATH = DATASQRL_CONFIG_DIRECTORY.resolve("auth");

  private static final long TIMEOUT_IN_SECONDS = 60;

  private final String state = getRandomString(16);
  private final String codeVerifier = getRandomString(43);
  private final String codeChallenge = generateCodeChallenge(codeVerifier);

  private final HttpClient client;

    public AuthProvider(HttpClient client) {
        this.client = client;
    }

    @SneakyThrows
  public String loginToRepository() {
    CompletableFuture<String> tokenFuture = new CompletableFuture<>();

    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new OAuthCallbackVerticle(code -> exchangeCodeForToken(code, tokenFuture)));

    openAuthWindow();

    try {
      String authToken = tokenFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);

      Files.createDirectories(DATASQRL_CONFIG_DIRECTORY);

      Files.write(AUTH_TOKEN_PATH, authToken.getBytes(StandardCharsets.UTF_8));

      return tokenFuture.get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException | ExecutionException | InterruptedException e) {
      throw new RuntimeException("Exception while getting access token.", e);
    } finally {
      vertx.close();
    }
  }

  @SneakyThrows
  public String isAuthenticated() {
    if (!Files.exists(AUTH_TOKEN_PATH)) return null;

    String cachedToken = Files.readString(AUTH_TOKEN_PATH);
    return isTokenValid(cachedToken) ? cachedToken : null;
  }

  private void exchangeCodeForToken(String code, CompletableFuture<String> tokenFuture) {
    JsonObject payload =
        new JsonObject()
            .put("grant_type", "authorization_code")
            .put("client_id", CLIENT_ID)
            .put("code_verifier", codeVerifier)
            .put("code", code)
            .put("redirect_uri", REDIRECT_URI);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(TOKEN_ENDPOINT))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
            .build();

    client
        .sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenAccept(
            response ->
                tokenFuture.complete(new JsonObject(response.body()).getString("access_token")))
        .exceptionally(
            throwable -> {
              tokenFuture.completeExceptionally(throwable);
              return null;
            });
  }

  private void openAuthWindow() throws IOException {
    Map<String, String> parameters =
        Map.of(
            "response_type", "code",
            "client_id", CLIENT_ID,
            "redirect_uri", URLEncoder.encode(REDIRECT_URI, StandardCharsets.UTF_8),
            "scope", "openid",
            "state", state,
            "audience", "https://sqrl-repository-frontend-git-staging-datasqrl.vercel.app/api/client",
            "code_challenge", codeChallenge,
            "code_challenge_method", "S256");

    String paramString =
        parameters.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining("&"));

    String authUrl = "https://" + AUTH0_DOMAIN + "/authorize?" + paramString;

    Runtime.getRuntime().exec("open " + authUrl);
  }

  @SneakyThrows
  private boolean isTokenValid(String authToken) {
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(USERINFO_ENDPOINT))
            .header("Authorization", "Bearer " + authToken)
            .GET()
            .timeout(Duration.of(10, ChronoUnit.SECONDS))
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    return response.statusCode() == 200;
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
