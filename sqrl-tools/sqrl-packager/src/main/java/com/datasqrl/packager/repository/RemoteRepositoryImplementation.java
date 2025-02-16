/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.repository;

import static com.datasqrl.auth.AuthUtils.REPO_URL;

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
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class RemoteRepositoryImplementation implements Repository, PublishRepository {

  private final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

  private final AuthProvider authProvider = new AuthProvider();

  private final URI repositoryServerURI;
  @Setter
  private CacheRepository cacheRepository = null;

  public RemoteRepositoryImplementation(URI repositoryServerURI) {
    this.repositoryServerURI = repositoryServerURI;
  }

  public RemoteRepositoryImplementation() {
    this(URI.create(REPO_URL));
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
          "File hash [%s] does not match hash" + "of downloaded file [%s]",
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

  @SneakyThrows
  public JsonNode getDependencyInfo(String name, String version, String variant) {
    HttpClient client = HttpClient.newHttpClient();

    Optional<String> authToken = authProvider.getAccessToken();

    URI uri = buildPackageInfoUri(name, version, variant);
	HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(uri);
    authToken.ifPresent((t) -> requestBuilder.header("Authorization", "Bearer " + t));
    requestBuilder.GET()
            .timeout(Duration.of(10, ChronoUnit.SECONDS))
            .build();

    HttpResponse<String> response;
	try {
		response = client.send(requestBuilder.build(), BodyHandlers.ofString());
	} catch (HttpConnectTimeoutException e) {
		HttpConnectTimeoutException error = new HttpConnectTimeoutException(String.format("HTTP connect timed out. uri: %s", repositoryServerURI));
		error.initCause(e);
		throw error;
	}
    int statusCode = response.statusCode();
    if (statusCode != 200) {
      String message =
          String.format(
              "Package [%s] is not available. Check if it exists and you have permission to access uri %s",
              name, uri);
      throw new RuntimeException(message);
    }
    return mapper.readValue(response.body(), JsonNode.class);
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
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      String authToken = authProvider.getAccessToken()
          .orElseThrow(() -> new RuntimeException("Must be logged in to publish. Run `sqrl login`"));

      HttpEntity httpEntity = createHttpEntity(zipFile, pkgConfig);
      HttpPost request = new HttpPost(repositoryServerURI.resolve("/api/packages").toString());
      request.setHeader("Authorization", "Bearer " + authToken);
      request.setEntity(httpEntity);

      try (CloseableHttpResponse response = client.execute(request)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          return true;
        } else {
          log.error("An error happened while uploading dependency: status code: {} response: {}",
              response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity()));
          return false;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to publish the package", e);
    }
  }

  private static HttpEntity createHttpEntity(Path zipFilePath, PackageConfiguration pkgConfig) {
    Map<String, Object> map = pkgConfig.toMap();

    MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() == null) continue;
      if (entry.getValue() instanceof List) {
        List<?> list = (List<?>) entry.getValue();
        for (int i = 0; i < list.size(); i++) {
          entityBuilder.addTextBody(String.format("%s[%d]", entry.getKey(), i), list.get(i).toString());
        }
      } else {
        entityBuilder.addTextBody(entry.getKey(), entry.getValue().toString());
      }
    }

    File zipFile = zipFilePath.toFile();
    entityBuilder.addBinaryBody("file", zipFile, ContentType.create("application/zip"), zipFile.getName());

    return entityBuilder.build();
  }
}
