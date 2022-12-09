package com.datasqrl.packager.repository;

import com.datasqrl.packager.FileHash;
import com.datasqrl.packager.config.Dependency;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;

public class RemoteRepositoryImplementation implements Repository {

  public static final URI DEFAULT_URI = URI.create("http://localhost:8888/graphql");

  private final ObjectMapper mapper = new ObjectMapper();
  private final URI repositoryServerURI;

  public RemoteRepositoryImplementation(URI repositoryServerURI) {
    this.repositoryServerURI = repositoryServerURI;
  }

  public RemoteRepositoryImplementation() {
    this(DEFAULT_URI);
  }

  @Override
  public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
    JsonNode result = executeQuery(Query.getLatest, Map.of(
        "pkgName", dependency.getName(),
        "version", dependency.getVersion(),
        "variant", dependency.getVariant()));
    Optional<JsonNode> version = getPackageField(result, "versions").filter(n -> n.isArray() && !n.isEmpty())
        .map(n -> n.get(0));
    if (version.isEmpty()) return false;
    JsonNode dep = version.get();
    String file = dep.get("file").asText();
    String hash = dep.get("hash").asText();
    Files.createDirectories(targetPath);

    Path zipFile = Files.createTempFile(targetPath, "package", ".zip");
    zipFile.toFile().deleteOnExit();
    String downloadHash = FileHash.getFor(zipFile);
    Preconditions.checkArgument(downloadHash.equals(hash),"File hash [%s] does not match hash"
            + "of dowloaded file [%s]", hash, downloadHash);
    FileUtils.copyURLToFile(
        new URL(file),
        zipFile.toFile());
    new ZipFile(zipFile.toFile()).extractAll(targetPath.toString());
    return true;
  }

  @Override
  public Optional<Dependency> resolveDependency(String packageName) {
    JsonNode result = executeQuery(Query.getLatest, Map.of("pkgName", packageName));
    Optional<JsonNode> latest = getPackageField(result, "latest").filter(n -> !n.isNull() && !n.isEmpty());
    if (latest.isEmpty()) return Optional.empty();
    return Optional.of(map(latest.get(), Dependency.class));
  }

  private Optional<JsonNode> getPackageField(JsonNode result, String field) {
    JsonNode packages = result.get("Package");
    if (!packages.isArray() || packages.isEmpty()) return Optional.empty();
    return Optional.of(packages.get(0).get("latest"));
  }

  public JsonNode executeQuery(Query query, Map<String,Object> payload) {
    try {
      HttpClient client = HttpClient.newHttpClient();
      HttpRequest request = HttpRequest.newBuilder()
          .POST(HttpRequest.BodyPublishers.ofString(mapper
              .writeValueAsString(
                  Map.of("query", query.getQueryString(), "variables", payload))))
          .uri(repositoryServerURI)
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
    URL url = Resources.getResource(queryFile);
    return Resources.toString(url, StandardCharsets.UTF_8);
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
