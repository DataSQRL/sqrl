/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.repository;

import com.datasqrl.auth.AuthProvider;
import com.datasqrl.config.Dependency;
import com.datasqrl.config.DependencyImpl;
import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.packager.util.FileHash;
import com.datasqrl.packager.util.Zipper;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import lombok.Setter;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;

public class RemoteRepositoryImplementation implements Repository, PublishRepository {
  public static final URI DEFAULT_URI = URI.create("https://sqrl-repository-frontend-git-staging-datasqrl.vercel.app"); // https://dev.datasqrl.com

  private final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
  private final HttpClient client = HttpClient.newHttpClient();
  private final AuthProvider authProvider = new AuthProvider(client);

  private final URI repositoryServerURI;
  @Setter
  private CacheRepository cacheRepository = null;

  public RemoteRepositoryImplementation(URI repositoryServerURI) {
    this.repositoryServerURI = repositoryServerURI;
  }

  public RemoteRepositoryImplementation() {
    this(DEFAULT_URI);
  }

  @Override
  public boolean retrieveDependency(Path targetPath, Dependency dependency) {
    JsonNode dependencyInfo = getDependencyInfo(dependency.getName(), dependency.getVersion().get(), dependency.getVariant());
    return downloadDependency(targetPath, dependencyInfo, dependency);
  }

  // Downloads the given Dependency to the specified Path
  private boolean downloadDependency(Path targetPath, JsonNode dependencyInfo, Dependency dependency) {
    String hash = dependencyInfo.get("hash").asText();
    String repoURL = dependencyInfo.get("repoURL").asText();

    try {
      // Create target directory
      Files.createDirectories(targetPath);

      // Create a temporary file for the zip file
      Path zipFile = Files.createTempFile(targetPath, "package", Zipper.ZIP_EXTENSION);

      // Copy the zip file from the repoURL to the temporary file
      FileUtils.copyURLToFile(new URL(repoURL), zipFile.toFile());

      // Get the hash for the downloaded file
      String downloadHash = FileHash.getFor(zipFile);

      // Ensure the hashes match
      Preconditions.checkArgument(
          downloadHash.equals(hash),
          "File hash [%s] does not match hash" + "of dowloaded file [%s]",
          hash,
          downloadHash);

      // Extract the zip file
      new ZipFile(zipFile.toFile()).extractAll(targetPath.toString());

      // Cache downloaded package
      if (cacheRepository != null) cacheRepository.cacheDependency(zipFile, dependency);

      // Delete the temporary file
      Files.deleteIfExists(zipFile);

      // Return true if the download was successful
      return true;
    } catch (Exception e) {
      // Return false if the download fails
      return false;
    }
  }

  @Override
  public Optional<Dependency> resolveDependency(String packageName) {
    JsonNode result = getLatestDependencyInfo(packageName);
    return Optional.of(map(result, DependencyImpl.class));
  }

  public JsonNode getDependencyInfo(String name, String version, String variant) {
    try {
      HttpClient client = HttpClient.newHttpClient();

      String authToken = authProvider.isAuthenticated();
      if (authToken == null) {
        authToken = authProvider.loginToRepository();
      }

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(buildPackageInfoUri(name, version, variant))
              .header("Authorization", "Bearer " + authToken)
              .GET()
              .timeout(Duration.of(10, ChronoUnit.SECONDS))
              .build();

      HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
      int statusCode = response.statusCode();
      if (statusCode != 200) {
        String message =
            String.format(
                "Could not call remote repository. statusCode=%d, body=%s",
                statusCode, response.body());
        throw new RuntimeException(message);
      }
      return mapper.readValue(response.body(), JsonNode.class);
    } catch (Exception e) {
      throw new RuntimeException("Could not call remote repository", e);
    }
  }

  public JsonNode getLatestDependencyInfo(String name) {
    return getDependencyInfo(name, null, null);
  }

  private URI buildPackageInfoUri(String name, String version, String variant) {
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }

    StringBuilder uriBuilder = new StringBuilder(repositoryServerURI.toString()).append("/api/packages/").append(name);

    // Append version and variant if provided
    if (version != null && variant != null) {
      uriBuilder.append("/").append(version).append("/").append(variant);
    }

    return URI.create(uriBuilder.toString());
  }

  private <O> O map(JsonNode node, Class<O> clazz) {
    try {
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      return mapper.treeToValue(node, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unexpected response from repository server: " + node.toString(), e);
    }
  }

  @Override
  @SneakyThrows
  public boolean publish(Path zipFile, PackageConfiguration pkgConfig) {
    HttpClient client = HttpClient.newHttpClient();

    String authToken = authProvider.isAuthenticated();
    if (authToken == null) {
      authToken = authProvider.loginToRepository();
    }

    HttpEntity httpEntity = createHttpEntity(zipFile, pkgConfig);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(repositoryServerURI.resolve("/api/packages"))
            .header("Content-Type", httpEntity.getContentType().getValue())
            .header("Authorization", "Bearer " + authToken)
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try {
                        return httpEntity.getContent();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }))
            .timeout(Duration.of(30, ChronoUnit.SECONDS))
            .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    if (response.statusCode() == 200) {
      return true;
    } else {
      System.err.printf("An error happened while uploading dependency: %s%n", response.body());
      return false;
    }
  }

  private static HttpEntity createHttpEntity(Path zipFilePath, PackageConfiguration pkgConfig) {
    File zipFile = zipFilePath.toFile();

    MultipartEntityBuilder entityBuilder =
        MultipartEntityBuilder.create()
            .addTextBody("name", pkgConfig.getName())
            .addTextBody("version", pkgConfig.getVersion())
            .addTextBody("latest", pkgConfig.getLatest().toString())
            .addTextBody("orgname", pkgConfig.getName().split("\\.", 2)[0])
            .addBinaryBody("file", zipFile, ContentType.create("application/zip"), zipFile.getName());

    String variant = pkgConfig.getVariant();
    if (variant != null) {
      entityBuilder.addTextBody("variant", variant);
    }

    String type = pkgConfig.getType();
    if (type != null) {
      entityBuilder.addTextBody("type", type);
    }

    String license = pkgConfig.getLicense();
    if (license != null) {
      entityBuilder.addTextBody("license", license);
    }
    String repository = pkgConfig.getRepository();
    if (repository != null) {
      entityBuilder.addTextBody("repository", repository);
    }
    String homepage = pkgConfig.getHomepage();
    if (homepage != null) {
      entityBuilder.addTextBody("homepage", homepage);
    }
    String documentation = pkgConfig.getDocumentation();
    if (documentation != null) {
      entityBuilder.addTextBody("documentation", documentation);
    }

    List<String> keywords = pkgConfig.getKeywords();
    for (int i = 0; i < keywords.size(); i++) {
      entityBuilder.addTextBody(String.format("topics[%d][name]", i), keywords.get(i));
    }

    return entityBuilder.build();
  }
}
