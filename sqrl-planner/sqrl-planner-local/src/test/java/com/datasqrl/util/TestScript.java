/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.util.data.Clickstream;
import com.datasqrl.util.data.Examples;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Repository;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Sensors;
import com.datasqrl.util.junit.ArgumentProvider;
import com.google.common.collect.ImmutableList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.Value;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public interface TestScript {

  String getName();

  Path getRootPackageDirectory();

  Path getScriptPath();

  @SneakyThrows
  default String getScript() {
    return Files.readString(getScriptPath());
  }

  List<String> getResultTables();

  List<TestGraphQLSchema> getGraphQLSchemas();

  default boolean dataSnapshot() {
    return true;
  }

  List<Path> getDataDirs();

  @Value
  @Builder
  static class Impl implements TestScript {

    @NonNull
    final String name;
    @NonNull
    final Path rootPackageDirectory;
    @NonNull
    final Path scriptPath;
    @NonNull
    @Singular
    final List<String> resultTables;
    @Builder.Default
    final boolean dataSnapshot = true;
    @Builder.Default
    @NonNull
    final List<TestGraphQLSchema> graphQLSchemas = List.of();

    @Singular
    final List<Path> dataDirs;
    @Override
    public String toString() {
      return name;
    }

    @Override
    public boolean dataSnapshot() {
      return dataSnapshot;
    }

  }

  static TestScript.Impl.ImplBuilder of(TestDataset dataset, Path script, String... resultTables) {
    return of(dataset.getRootPackageDirectory(), script, resultTables);
  }

  static TestScript.Impl.ImplBuilder of(Path rootPackage, Path script, String... resultTables) {
    String name = script.getFileName().toString();
    if (name.endsWith(".sqrl")) {
      name = StringUtil.removeFromEnd(name, ".sqrl");
    }
    return Impl.builder().name(name).rootPackageDirectory(rootPackage).scriptPath(script)
        .resultTables(Arrays.asList(resultTables));
  }

        /*
    === STATIC METHODS ===
     */

  static List<TestScript> getAll() {
    return ImmutableList.<TestScript>builder()
        .addAll(Retail.INSTANCE.getTestScripts().values())
        .addAll(Nutshop.INSTANCE.getScripts().subList(0, 2))
        .addAll(Quickstart.INSTANCE.getScripts())
        .addAll(Clickstream.INSTANCE.getScripts())
        .addAll(Sensors.INSTANCE.getScripts())
//        .addAll(Repository.INSTANCE.getScripts())
        .build();
  }

  class AllScriptsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return ArgumentProvider.of(getAll());
    }
  }

  public class AllScriptsWithGraphQLSchemaProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return getAll().stream().flatMap(script ->
          script.getGraphQLSchemas().stream().map(gql -> Arguments.of(script, gql)));
    }
  }

  public class AllScriptsWithAllEnginesProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      List<TestScript> engineScripts = ImmutableList.<TestScript>builder()
          .addAll(Retail.INSTANCE.getTestScripts().values())
          .add(Nutshop.INSTANCE.getScripts().get(1))
          .addAll(Quickstart.INSTANCE.getScripts())
          .addAll(Clickstream.INSTANCE.getScripts())
          .addAll(Sensors.INSTANCE.getScripts())
          .build();
      return engineScripts.stream()
          .flatMap(script ->
              script.getGraphQLSchemas().stream()
                  .flatMap(gql ->
                          jdbcEngines.stream()
                                  .map(e -> Arguments.of(script, gql, e))));
    }
  }

  public class ExampleScriptsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {

      return Examples.scriptList.stream()
          .map(script->Arguments.of(script));
    }
  }

  List<DatabaseEngine> jdbcEngines = List.of(DatabaseEngine.H2,
      DatabaseEngine.POSTGRES, DatabaseEngine.SQLITE);

}
