package com.datasqrl.service;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class PathUtil {
  public static final Path DEFAULT_PACKAGE = Path.of("package.json");

  public static List<Path> getOrCreateDefaultPackageFiles(RootCommand root) {
    Optional<List<Path>> existingPackageJson = PathUtil.findPackageFiles(root.getPackageFiles());
    return existingPackageJson
        .orElseGet(() -> List.of(writeEngineConfig(root.getRootDir(),
            createDefaultConfig())));
  }

  @SneakyThrows
  protected static Path writeEngineConfig(Path rootDir, GlobalEngineConfiguration config) {
    Path enginesFile = Files.createTempFile(rootDir, "package-engines", ".json");
    File file = enginesFile.toFile();
    file.deleteOnExit();

    ObjectMapper mapper = new ObjectMapper();
    String enginesConf = mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(config);

    Files.write(enginesFile, enginesConf.getBytes(StandardCharsets.UTF_8));
    return enginesFile;
  }

  protected static GlobalEngineConfiguration createDefaultConfig() {
    JDBCEngineConfiguration jdbcEngineConfiguration = JDBCEngineConfiguration.builder()
        .config(JdbcDataSystemConnectorConfig.builder()
            .dbURL("jdbc:h2:file:./h2.db")
            .driverName("org.h2.Driver")
            .dialect("h2")
            .database("datasqrl")
            .build())
        .build();

    FlinkEngineConfiguration flinkEngineConfiguration =
        FlinkEngineConfiguration.builder()
            .savepoint(false)
            .build();

    GlobalEngineConfiguration engineConfiguration = GlobalEngineConfiguration.builder()
        .engines(List.of(flinkEngineConfiguration, jdbcEngineConfiguration))
        .build();
    return engineConfiguration;
  }

  public static Optional<List<Path>> findPackageFiles(List<Path> packageFiles) {
    if (packageFiles.isEmpty()) {
      if (Files.isRegularFile(DEFAULT_PACKAGE)) {
        return Optional.of(List.of(DEFAULT_PACKAGE));
      }
    }
    return packageFiles.isEmpty() ? Optional.empty() :
        Optional.of(packageFiles);
  }
}
