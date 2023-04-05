package com.datasqrl.util.data;

import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class UseCaseExample implements TestDataset {

  public static final Path BASE_PATH = Path.of("..", "sqrl-examples");

  protected final String name;
  protected final Path basePath;
  protected final Set<String> tableNames;
  protected final ListMultimap<String,String> scriptsWithTables;

  protected final String variant;

  protected UseCaseExample(Set<String> tableNames) {
    this(tableNames, ArrayListMultimap.create());
  }

  protected UseCaseExample(Set<String> tableNames,
      ListMultimap<String,String> scriptsWithTables) {
    this("",tableNames, scriptsWithTables);
  }

  protected UseCaseExample(String variant, Set<String> tableNames,
      ListMultimap<String,String> scriptsWithTables) {
    this.name = getClass().getSimpleName().toLowerCase();
    this.basePath = BASE_PATH.resolve(name);
    this.tableNames = tableNames;
    this.scriptsWithTables = scriptsWithTables;
    this.variant = variant;
    Preconditions.checkArgument(scriptsWithTables.keySet().stream()
        .noneMatch(s -> s.endsWith("sqrl")),"Scripts should"
        + "be listed without .sqrl extension");
  }

  @Override
  public String getName() {
    return name;
  }

  private String getVariant() {
    return Strings.isNullOrEmpty(variant)?"":"-"+ variant;
  }

  @Override
  public Path getDataDirectory() {
    return basePath.resolve("data"+ getVariant());
  }

  @Override
  public Set<String> getTables() {
    return tableNames;
  }

  @Override
  public Path getRootPackageDirectory() {
    return basePath;
  }

  @Override
  public Path getDataPackageDirectory() {
    return getRootPackageDirectory().resolve("mySourcePackage"+ getVariant());
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  @SneakyThrows
  public DataSystemDiscoveryConfig getDiscoveryConfig() {
//    String baseUrl = "https://github.com/DataSQRL/sqrl/raw/651b944d4597865cf020c8fb8b73aca18aa1c3ca/sqrl-examples/quickstart/data/%s";
    List<String> urls;
    try (Stream<Path> fileStream = Files.list(getDataDirectory())) {
      urls = fileStream.filter(Files::isRegularFile).
          map(Path::toString).collect(Collectors.toList());
    }
    return DirectoryDataSystemConfig.Discovery.builder()
        .fileURIs(urls)
        .build();
  }

  public List<TestScript> getScripts() {
    return scriptsWithTables.keySet().stream().map(scriptName -> {
        scriptName = scriptName + getVariant();
        return TestScript.Impl.builder().rootPackageDirectory(getRootPackageDirectory())
            .name(scriptName).scriptPath(getRootPackageDirectory().resolve(scriptName+".sqrl"))
            .resultTables(scriptsWithTables.get(scriptName))
            .dataSnapshot(false)
            .graphQLSchemas(getGraphQLSchema(scriptName))
            .build();
        }).collect(Collectors.toList());
  }

  @SneakyThrows
  private List<TestGraphQLSchema> getGraphQLSchema(String scriptName) {
    try (Stream<Path> fileStream = Files.list(getRootPackageDirectory())) {
      return fileStream.filter(Files::isDirectory).filter(dir -> {
        String dirName = dir.getFileName().toString();
        return dirName.toLowerCase().startsWith(scriptName.toLowerCase()+"-graphql");
      }).map(TestGraphQLSchema.Directory::new).collect(Collectors.toList());
    }
  }

  public static ListMultimap script(String scriptName, String... resultTables) {
    Preconditions.checkArgument(resultTables.length>0, "Need to specify result tables");
    ListMultimap<String,String> result = ArrayListMultimap.create();
    for (String resultTable : resultTables) {
      result.put(scriptName,resultTable);
    }
    return result;
  }

}
