/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.util.data.Clickstream;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Repository;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
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

    @NonNull final String name;
    @NonNull final Path rootPackageDirectory;
    @NonNull final Path scriptPath;
    @NonNull @Singular final List<String> resultTables;
    @Builder.Default final boolean dataSnapshot = true;
    @Builder.Default @NonNull final List<TestGraphQLSchema> graphQLSchemas = List.of();

    @Singular final List<Path> dataDirs;

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
    var name = script.getFileName().toString();
    if (name.endsWith(".sqrl")) {
      name = StringUtil.removeFromEnd(name, ".sqrl");
    }
    return Impl.builder()
        .name(name)
        .rootPackageDirectory(rootPackage)
        .scriptPath(script)
        .resultTables(Arrays.asList(resultTables));
  }

  /*
  === STATIC METHODS ===
   */

  class PhysicalUseCaseProvider implements ArgumentsProvider {

    private static List<TestScript> SCRIPTS =
        ImmutableList.<TestScript>builder()
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.ORDER_STATS))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.FULL))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.RECOMMEND))
            .addAll(Nutshop.INSTANCE.getScripts().subList(0, 2))
            .addAll(Quickstart.INSTANCE.getScripts())
            .addAll(Clickstream.INSTANCE.getScripts())
            .addAll(Sensors.INSTANCE.getScripts())
            .addAll(Repository.INSTANCE.getScripts())
            .build();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return ArgumentProvider.of(SCRIPTS);
    }
  }

  public class QueryUseCaseProvider implements ArgumentsProvider {

    private static List<TestScript> SCRIPTS =
        ImmutableList.<TestScript>builder()
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.ORDER_STATS))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.FULL))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.RECOMMEND))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.SEARCH))
            .addAll(Nutshop.INSTANCE.getScripts().subList(0, 2))
            .addAll(Quickstart.INSTANCE.getScripts())
            .addAll(Clickstream.INSTANCE.getScripts())
            .add(Sensors.INSTANCE.getScripts().get(0))
            .addAll(Repository.INSTANCE.getScripts())
            .build();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return SCRIPTS.stream()
          .flatMap(
              script -> script.getGraphQLSchemas().stream().map(gql -> Arguments.of(script, gql)));
    }
  }

  class AllScriptsWithAllEnginesProvider implements ArgumentsProvider {

    private static List<TestScript> SCRIPTS =
        ImmutableList.<TestScript>builder()
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.ORDER_STATS))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.FULL))
            .add(Retail.INSTANCE.getTestScripts().get(RetailScriptNames.RECOMMEND))
            .add(Nutshop.INSTANCE.getScripts().get(1))
            .addAll(Quickstart.INSTANCE.getScripts())
            .addAll(Clickstream.INSTANCE.getScripts())
            .add(Sensors.INSTANCE.getScripts().get(0))
            .build();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {

      return SCRIPTS.stream()
          .flatMap(
              script ->
                  script.getGraphQLSchemas().stream()
                      .flatMap(gql -> jdbcEngines.stream().map(e -> Arguments.of(script, gql, e))));
    }
  }

  List<DatabaseEngine> jdbcEngines = List.of(DatabaseEngine.POSTGRES);
}
