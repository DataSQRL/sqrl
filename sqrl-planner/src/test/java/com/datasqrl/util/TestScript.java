/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.util.data.DataSQRLRepo;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
    final List<String> resultTables;
    @Builder.Default
    final boolean dataSnapshot = true;
    @Builder.Default
    @NonNull
    final List<TestGraphQLSchema> graphQLSchemas = List.of();

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
      name = name.substring(0, name.length() - 5);
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
        .add(DataSQRLRepo.INSTANCE.getScript())
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

      return getAll().stream()
          .flatMap(script ->
              script.getGraphQLSchemas().stream()
                  .flatMap(gql ->
                          jdbcEngines.stream()
                                  .map(e -> Arguments.of(script, gql, e))));
    }
  }

  List<DatabaseEngine> jdbcEngines = List.of(DatabaseEngine.H2,
      DatabaseEngine.POSTGRES, DatabaseEngine.SQLITE);

}
