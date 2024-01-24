package com.datasqrl.postprocess;

import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.packager.Packager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;

public class DefaultPost {
//
//  protected void postCompileActions(DefaultConfigSupplier configSupplier, Packager packager,
//      CompilerResult result, ErrorCollector errors) {
//    if (configSupplier.usesDefault) {
//      addDockerCompose(Optional.ofNullable(mountDirectory));
//      addFlinkExecute();
//      addInitFlink();
//    }
//    if (isGenerateGraphql()) {
//      addGraphql(packager.getBuildDir(), packager.getRootDir());
//    }
//  }
//
//  protected boolean isGenerateGraphql() {
//    if (generateAPI != null) {
//      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
//    }
//    return false;
//  }
//
//  @SneakyThrows
//  protected void addGraphql(Path build, Path rootDir) {
//    Files.copy(build.resolve(GRAPHQL_NORMALIZED_FILE_NAME),
//        rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME));
//  }
//
//  //Adds in regardless

//
//  protected void addFlinkExecute() {
//    String content = DockerCompose.getFlinkExecute();
//    copyExecutableFile("submit-flink-job.sh", content);
//  }
//
//  protected void addInitFlink() {
//    String content = DockerCompose.getInitFlink();
//    copyExecutableFile("init-flink.sh", content);
//  }
//
//  protected void copyExecutableFile(String fileName, String content) {
//    Path toFile = targetDir.resolve(fileName);
//    try {
//      Files.createDirectories(targetDir);
//      Files.writeString(toFile, content);
//
//      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
//      Files.setPosixFilePermissions(toFile, perms);
//
//    } catch (Exception e) {
//      log.error("Could not copy flink executor file.");
//      throw new RuntimeException(e);
//    }
//  }
}
