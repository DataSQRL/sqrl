package com.datasqrl.util;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import lombok.SneakyThrows;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.testcontainers.shaded.org.bouncycastle.util.Pack;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCompiler {

  public Path compile(Path rootDir, Path packageOverride) {
    Optional<Path> script = findFile(rootDir, ".sqrl");
    Optional<Path> graphql = findFile(rootDir, ".graphqls");

    if (script.isEmpty() && graphql.isEmpty()) {
      throw new RuntimeException("Could not find file: script<" + script + "> graphqls<" + graphql + ">");
    }
    compile(rootDir, packageOverride, script.get(), graphql.get());
    return rootDir;
  }

  @SneakyThrows
  public Path compile(Path rootDir, Path packageOverride, Path script, Path graphql) {
    Path defaultPackage = createDefaultPackage(rootDir, script, graphql);

    picocli.CommandLine rootCommand = new RootCommand(rootDir, AssertStatusHook.INSTANCE).getCmd();
    int code = rootCommand.execute("compile", script.toString(), graphql.toString(),
        "-c", defaultPackage.toAbsolutePath().toString(),
        "-c", packageOverride.toAbsolutePath().toString(),
        "--nolookup");
    assertEquals(0, code, "Non-zero exit code");
    return rootDir;
  }

  @SneakyThrows
  public Optional<Path> findFile(Path rootDir, String postfix) {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir)) {
      for (Path file : stream) {
        if (!Files.isDirectory(file) && file.getFileName().toString().endsWith(postfix)) {
          return Optional.of(file);
        }
      }
    }
    return Optional.empty();
  }


  @SneakyThrows
  public Path createDefaultPackage(Path rootDir, Path script, Path graphql) {
    SqrlConfig config = Packager.createDockerConfig(ErrorCollector.root());
    Packager.setScriptFiles(rootDir, script, graphql, config, ErrorCollector.root());

    Path defaultPackage = Files.createTempFile("pkJson", ".json");
    config.toFile(defaultPackage, true);
    defaultPackage.toFile().deleteOnExit();
    return defaultPackage;
  }
}
