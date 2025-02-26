package com.datasqrl.auth;

import com.datasqrl.util.FileUtil;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;

public class TokenManager {

  private static final Path DATASQRL_CONFIG_DIRECTORY = FileUtil.getUserRoot().resolve(".datasqrl");
  private static final Path REFRESH_TOKEN_PATH = DATASQRL_CONFIG_DIRECTORY.resolve("auth");
  private static final String ENV_DATASQRL_TOKEN = "DATASQRL_TOKEN";
  private String accessToken;

  @SneakyThrows
  public Optional<String> getRefreshToken() {
    String refreshTokenFromEnv = System.getenv(ENV_DATASQRL_TOKEN);
    if (refreshTokenFromEnv != null && !refreshTokenFromEnv.isEmpty()) {
      return Optional.of(refreshTokenFromEnv);
    }

    return Files.isRegularFile(REFRESH_TOKEN_PATH)
        ? Optional.of(Files.readString(REFRESH_TOKEN_PATH))
        : Optional.empty();
  }

  @SneakyThrows
  public void setRefreshToken(String refreshToken) {
    Files.createDirectories(DATASQRL_CONFIG_DIRECTORY);
    Files.write(REFRESH_TOKEN_PATH, refreshToken.getBytes(StandardCharsets.UTF_8));
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public Optional<String> getAccessToken() {
    return accessToken == null ? Optional.empty() : Optional.of(accessToken);
  }
}
