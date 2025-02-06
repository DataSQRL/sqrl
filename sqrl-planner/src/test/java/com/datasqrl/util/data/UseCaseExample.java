package com.datasqrl.util.data;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datasqrl.util.TestDataset;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import lombok.SneakyThrows;

public class UseCaseExample implements TestDataset {

  public static final String DATA_DIR = "data";

  public static final String DATA_PACKAGE = "mysourcepackage";
  public static final Path BASE_PATH = Path.of("..", "..", "sqrl-examples");

  protected final String name;
  protected final Path basePath;
  protected final Set<String> tableNames;
  protected final ListMultimap<String,String> scriptsWithTables;

  protected final String variant;
  protected final String graphqlSchema;

  protected UseCaseExample(Set<String> tableNames) {
    this(tableNames, ArrayListMultimap.create());
  }

  protected UseCaseExample(Set<String> tableNames,
      ListMultimap<String,String> scriptsWithTables) {
    this("",tableNames, scriptsWithTables);
  }

  protected UseCaseExample(String variant, Set<String> tableNames,
      ListMultimap<String,String> scriptsWithTables) {
    this(variant, tableNames, scriptsWithTables, null);
  }

  protected UseCaseExample(String variant, Set<String> tableNames,
                           ListMultimap<String,String> scriptsWithTables,
                           String graphqlSchema) {
    this.name = getClass().getSimpleName().toLowerCase();
    this.basePath = BASE_PATH.resolve(name);
    this.tableNames = tableNames;
    this.scriptsWithTables = scriptsWithTables;
    this.variant = variant;
    this.graphqlSchema = graphqlSchema;
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
    return basePath.resolve(DATA_DIR+ getVariant());
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
    return getRootPackageDirectory().resolve(DATA_PACKAGE + getVariant());
  }

  @Override
  public String toString() {
    return name;
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
    return scripts().add(scriptName, resultTables).build();
  }

  public static ScriptBuilder scripts() {
    return new ScriptBuilder();
  }

  public static class ScriptBuilder {

    ListMultimap<String,String> result = ArrayListMultimap.create();

    public ScriptBuilder add(String scriptName, String... resultTables) {
      Preconditions.checkArgument(resultTables.length>0, "Need to specify result tables");
      for (String resultTable : resultTables) {
        result.put(scriptName,resultTable);
      }
      return this;
    }

    public ListMultimap build() {
      return result;
    }

  }

  public Path getGraphqlSchemaPath() {
    Preconditions.checkNotNull(graphqlSchema, "Expected graphql schema, found none.");
    return getRootPackageDirectory()
        .resolve(graphqlSchema);
  }
}
