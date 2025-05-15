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
// package com.datasqrl.util;
//
// import com.datasqrl.cmd.AssertStatusHook;
// import com.datasqrl.cmd.RootCommand;
// import com.datasqrl.error.ErrorCollector;
// import com.datasqrl.packager.Packager;
// import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
// import lombok.SneakyThrows;
//
// import java.nio.file.DirectoryStream;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.Optional;
// import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
// import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
// import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
//
// import static com.datasqrl.packager.Packager.setScriptFiles;
// import static org.junit.jupiter.api.Assertions.assertEquals;
//
// public class TestCompiler {
//
//  public Path compile(Path rootDir, Path packageOverride) {
//    Optional<Path> script = findFile(rootDir, ".sqrl");
//    Optional<Path> graphql = findFile(rootDir, ".graphqls");
//
//    if (script.isEmpty() && graphql.isEmpty()) {
//      throw new RuntimeException("Could not find file: script<" + script + "> graphqls<" + graphql
// + ">");
//    }
//    compile(rootDir, packageOverride, script.get(), graphql.get());
//    return rootDir;
//  }
//
//  @SneakyThrows
//  public Path compile(Path rootDir, Path packageOverride, Path script, Path graphql) {
//    Path defaultPackage = createDefaultPackage(rootDir, script, graphql);
//
//    picocli.CommandLine rootCommand = new RootCommand(rootDir,
// AssertStatusHook.INSTANCE).getCmd();
//    int code = rootCommand.execute("compile", script.toString(), graphql.toString(),
//        "-c", defaultPackage.toAbsolutePath().toString(),
//        "-c", packageOverride.toAbsolutePath().toString());
//    assertEquals(0, code, "Non-zero exit code");
//    return rootDir;
//  }
//
//  @SneakyThrows
//  public Optional<Path> findFile(Path rootDir, String postfix) {
//    try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir)) {
//      for (Path file : stream) {
//        if (!Files.isDirectory(file) && file.getFileName().toString().endsWith(postfix)) {
//          return Optional.of(file);
//        }
//      }
//    }
//    return Optional.empty();
//  }
//
//  @SneakyThrows
//  public Path createDefaultPackage(Path rootDir, Path script, Path graphql) {
//    SqrlConfig config = Packager.createDockerConfig(ErrorCollector.root());
//    setScriptFiles(rootDir, script, graphql, config, ErrorCollector.root());
//
//    Path defaultPackage = Files.createTempFile("pkJson", ".json");
//    config.toFile(defaultPackage, true);
//    defaultPackage.toFile().deleteOnExit();
//    return defaultPackage;
//  }
// }
