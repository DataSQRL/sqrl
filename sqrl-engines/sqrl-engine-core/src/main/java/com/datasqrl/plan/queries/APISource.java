package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.Model.PreparsedQuery;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.util.FileUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Slf4j
public class APISource {

  @Include
  Name name;
  String schemaDefinition;
  List<String> preparsedQueries;

  private APISource(Name name, String schemaDefinition, List<String> preparsedQueries) {
    this.name = name;
    this.schemaDefinition = schemaDefinition;
    this.preparsedQueries = preparsedQueries;
  }

  public static APISource of(String schemaDefinition) {
    return of(Name.system("schema"), schemaDefinition);
  }

  public static APISource of(Name name, String schemaDefinition) {
    return of(name, schemaDefinition, List.of());
  }

  public static APISource of(Name name, String schemaDefinition, List<String> queries) {
    return new APISource(name,
        schemaDefinition.replaceAll("\t", "  "),
        queries);
  }

  public static APISource of(String filename, NameCanonicalizer canonicalizer, ResourceResolver resolver) {

    Path path = Path.of(resolver.resolveFile(NamePath.of(filename)).get());
    String fileName = path.getFileName().toString().split("\\.")[0];

    Path queryFolderPath = path.getParent().resolve(fileName + "-queries");
    List<String> queries = new ArrayList<>();
    if (Files.isDirectory(queryFolderPath)) {
      try (Stream<Path> paths = Files.walk(queryFolderPath)) {
        paths.filter(Files::isRegularFile)
            .filter(p -> p.toString().endsWith(".graphql"))
            .forEach(p -> {
              try {
                String content = Files.readString(p);
                queries.add(content);
              } catch (IOException e) {
                log.error("Could not read from filesystem", e);
              }
            });
      } catch (IOException e) {
        log.error("Could not read from filesystem", e);
      }
    }

    URI uri = resolver.resolveFile(NamePath.of(filename)).get();
    return APISource.of(
        canonicalizer.name(FileUtil.separateExtension(filename).getKey()),
        FileUtil.readFile(uri),
        queries);
  }

  @Override
  public String toString() {
    return name.toString();
  }

}
