/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.repository;


import com.datasqrl.config.DependencyImpl;
import com.datasqrl.config.Dependency;
import com.datasqrl.packager.util.FileHash;
import com.datasqrl.packager.util.Zipper;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import lombok.Setter;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class RemoteRepositoryImplementation implements Repository {
  public static final String PKG_NAME_KEY = "pkgName";
  public static final String VERSION_KEY = "version";
  public static final String VARIANT_KEY = "variant";

  public static final URI DEFAULT_URI = URI.create("https://repo.sqrl.run");

  private final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
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
  public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
    JsonNode result = executeQuery(Query.getDependency, Map.of(
        PKG_NAME_KEY, dependency.getName(),
        VERSION_KEY, dependency.getVersion(),
        VARIANT_KEY, dependency.getVariant()));
    return getDependencyVersion(result)
        .map(dep -> downloadDependency(targetPath, dep, dependency))
        .orElse(false);
  }

  // Gets the first dependency version from a retrieved package
  private Optional<JsonNode> getDependencyVersion(JsonNode result) {
    return getPackageField(result, "versions")
        .filter(n -> n.isArray() && !n.isEmpty())
        .map(n -> n.get(0));
  }

  // Downloads the given Dependency to the specified Path
  private boolean downloadDependency(Path targetPath, JsonNode dep, Dependency dependency) {
    String file = dep.get("file").asText();
    String hash = dep.get("hash").asText();
    String repoURL = dep.get("repoURL").asText();

    try {
      // Create target directory
      Files.createDirectories(targetPath);

      // Create a temporary file for the zip file
      Path zipFile = Files.createTempFile(targetPath, "package", Zipper.ZIP_EXTENSION);

      // Copy the zip file from the repoURL to the temporary file
      FileUtils.copyURLToFile(
          new URL(repoURL),
          zipFile.toFile());

      // Get the hash for the downloaded file
      String downloadHash = FileHash.getFor(zipFile);

      // Ensure the hashes match
      Preconditions.checkArgument(downloadHash.equals(hash),"File hash [%s] does not match hash"
          + "of dowloaded file [%s]", hash, downloadHash);

      // Extract the zip file
      new ZipFile(zipFile.toFile()).extractAll(targetPath.toString());

      // Cache downloaded package
      if (cacheRepository!=null) cacheRepository.cacheDependency(zipFile, dependency);

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
    JsonNode result = executeQuery(Query.getLatest, Map.of("pkgName", packageName));
    Optional<JsonNode> latest = getPackageField(result, "latest").filter(n -> !n.isNull() && !n.isEmpty());
    if (latest.isEmpty()) return Optional.empty();
    return Optional.of(map(latest.get(), DependencyImpl.class));
  }

  private Optional<JsonNode> getPackageField(JsonNode result, String field) {
    JsonNode packages = result.get("Package");
    if (!packages.isArray() || packages.isEmpty()) return Optional.empty();
    return Optional.of(packages.get(0).get(field));
  }

  public JsonNode executeQuery(Query query, Map<String,Object> payload) {
    try {
      HttpClient client = HttpClient.newHttpClient();
      HttpRequest request = HttpRequest.newBuilder()
          .header("Content-Type", "application/json")
          .header("Accept", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(mapper
              .writeValueAsString(
                  Map.of("query", query.getQueryString(), "variables", payload))))
          .uri(repositoryServerURI)
          .timeout(Duration.of(10, ChronoUnit.SECONDS))
          .build();
      HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
      return mapper.readValue(response.body(), JsonNode.class).get("data");
    } catch (Exception e) {
      throw new RuntimeException("Could not call remote repository",e);
    }
  }

  private<O> O map(JsonNode node, Class<O> clazz) {
    try {
      return mapper.treeToValue(node,clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unexpected response from repository server: " + node.toString(), e);
    }
  }

  @SneakyThrows
  private static String loadQuery(String queryFile) {
    return FileUtil.readResource(queryFile);
  }

  private enum Query {

    getDependency("getDependency.graphql"), getLatest("latestPackageByName.graphql");

    private final String fileName;
    private String queryString;

    Query(String fileName) {
      this.fileName = fileName;
    }

    public synchronized String getQueryString() {
      if (queryString==null) {
        queryString = loadQuery(fileName);
      }
      return queryString;
    }

  }

}
