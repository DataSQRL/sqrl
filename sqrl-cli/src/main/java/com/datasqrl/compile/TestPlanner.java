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
package com.datasqrl.compile;

import com.datasqrl.compile.TestPlan.GraphqlQuery;
import com.datasqrl.config.PackageJson;
import com.datasqrl.graphql.APISource;
import com.datasqrl.util.FileUtil;
import graphql.language.AstPrinter;
import graphql.language.Definition;
import graphql.language.Document;
import graphql.language.Node;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/** Constructs test plans with pre-computed headers and test information. */
@RequiredArgsConstructor
public class TestPlanner {

  private final PackageJson packageJson;
  private final GqlGenerator gqlGenerator;

  public TestPlan generateTestPlan(APISource source, Optional<Path> testsPath) {
    var parser = new Parser();
    List<GraphqlQuery> queries = new ArrayList<>();
    List<GraphqlQuery> mutations = new ArrayList<>();
    List<GraphqlQuery> subscriptions = new ArrayList<>();

    // Get base headers from PackageJson
    var baseHeaders = packageJson.getTestConfig().getHeaders();

    testsPath.ifPresent(
        p -> {
          try (var paths = Files.walk(p)) {
            paths
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".graphql"))
                .sorted(Comparator.comparing(path -> path.getFileName().toString().toLowerCase()))
                .forEach(
                    file -> {
                      String content = null;
                      try {
                        content = new String(Files.readAllBytes(file));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      var document = parser.parseDocument(content);
                      var prefix = FileUtil.separateExtension(file).getLeft();
                      // TODO extract subscriptions from .graphql files
                      extractQueriesAndMutations(
                          document,
                          queries,
                          mutations,
                          subscriptions,
                          prefix,
                          loadHeaders(file.getParent(), prefix, baseHeaders));
                    });
          } catch (IOException e) {
            e.printStackTrace();
          }
        });

    var document = parser.parseDocument(source.getDefinition());
    var queryNodes = gqlGenerator.visitDocument(document, null);
    for (Node definition : queryNodes) {
      var definition1 = (OperationDefinition) definition;
      queries.add(
          new GraphqlQuery(definition1.getName(), AstPrinter.printAst(definition1), baseHeaders));
    }
    return new TestPlan(queries, mutations, subscriptions);
  }

  @SneakyThrows
  public Map<String, String> loadHeaders(
      Path testDir, String prefix, Map<String, String> baseHeaders) {
    var headersFile = testDir.resolve(prefix + ".properties");

    if (!Files.isRegularFile(headersFile)) {
      return baseHeaders;
    }

    var props = readProperties(headersFile);

    // Combine base headers with file-specific headers
    return combineHeaders(baseHeaders, props);
  }

  @SneakyThrows
  private Properties readProperties(Path p) {
    Properties props = new Properties();
    try (var in = Files.newInputStream(p)) {
      props.load(in);
      return props;
    }
  }

  private Map<String, String> combineHeaders(
      Map<String, String> baseHeaders, Properties overrides) {
    var headers = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    headers.putAll(baseHeaders);

    if (overrides != null && !overrides.isEmpty()) {
      overrides.forEach((key, value) -> headers.put(key.toString(), value.toString()));
    }

    return headers;
  }

  private void extractQueriesAndMutations(
      Document document,
      List<GraphqlQuery> queries,
      List<GraphqlQuery> mutations,
      List<GraphqlQuery> subscriptions,
      String prefix,
      Map<String, String> headers) {
    for (Definition definition : document.getDefinitions()) {
      if (definition instanceof OperationDefinition operationDefinition) {
        var query = new GraphqlQuery(prefix, AstPrinter.printAst(operationDefinition), headers);
        switch (operationDefinition.getOperation()) {
          case QUERY:
            queries.add(query);
            break;
          case MUTATION:
            mutations.add(query);
            break;
          case SUBSCRIPTION:
            subscriptions.add(query);
            break;
        }
      }
    }
  }
}
