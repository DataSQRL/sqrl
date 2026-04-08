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

import eu.maveniverse.maven.mima.context.Context;
import eu.maveniverse.maven.mima.context.ContextOverrides;
import eu.maveniverse.maven.mima.context.Runtimes;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.util.graph.visitor.PreorderNodeListGenerator;

@Slf4j
public class MavenDependencyResolver {

  private static final Pattern JDEPS_PATTERN =
      Pattern.compile("^//JDEPS\\s+(.+)$", Pattern.MULTILINE);
  private static final Pattern FLINK_JDEPS_PATTERN =
      Pattern.compile("^//JDEPS\\s+org\\.apache\\.flink:", Pattern.MULTILINE);

  public List<String> parseJdeps(String sourceContent) {
    var matcher = JDEPS_PATTERN.matcher(sourceContent);
    var deps = new ArrayList<String>();
    while (matcher.find()) {
      deps.add(matcher.group(1).trim());
    }
    return deps;
  }

  public void validateNoFlinkJdeps(Path file, String sourceContent) {
    if (FLINK_JDEPS_PATTERN.matcher(sourceContent).find()) {
      throw new IllegalArgumentException(
          "File "
              + file
              + " declares a Flink //JDEPS dependency. "
              + "Flink dependencies are provided automatically via classpath and must not be declared in //JDEPS.");
    }
  }

  public List<Path> resolve(List<String> gavCoordinates) {
    if (gavCoordinates.isEmpty()) {
      return List.of();
    }

    log.info("Resolving {} dependencies via Mima: {}", gavCoordinates.size(), gavCoordinates);

    var runtime = Runtimes.INSTANCE.getRuntime();
    var overrides = ContextOverrides.create().withUserSettings(true).build();

    try (Context context = runtime.create(overrides)) {
      var allFiles = new ArrayList<File>();

      for (var gav : gavCoordinates) {
        var artifact = new DefaultArtifact(gav);
        var collectRequest = new CollectRequest();
        collectRequest.setRoot(new Dependency(artifact, "runtime"));
        collectRequest.setRepositories(context.remoteRepositories());

        var request = new DependencyRequest();
        request.setCollectRequest(collectRequest);

        var rootNode =
            context
                .repositorySystem()
                .resolveDependencies(context.repositorySystemSession(), request)
                .getRoot();

        var nlg = new PreorderNodeListGenerator();
        rootNode.accept(nlg);
        allFiles.addAll(nlg.getFiles());
      }

      var paths = allFiles.stream().distinct().map(File::toPath).collect(Collectors.toList());

      log.info("Resolved {} JARs", paths.size());
      return paths;

    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve dependencies: " + gavCoordinates, e);
    }
  }
}
