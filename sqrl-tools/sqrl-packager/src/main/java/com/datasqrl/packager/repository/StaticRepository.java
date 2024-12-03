package com.datasqrl.packager.repository;

import com.datasqrl.config.Dependency;
import com.datasqrl.config.DependencyImpl;
import com.datasqrl.config.PackageConfigurationImpl;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import net.lingala.zip4j.ZipFile;
import lombok.Value;

public class StaticRepository implements Repository {

  private static final String VERSION = "0.5.9"; //TODO: Automatically replace during release or read from somewhere?
  private static final String RESOURCE_DIR = "profiles/";
  private static final List<StaticDependency> STATIC_DEPENDENCIES = List.of(
      StaticDependency.of("datasqrl.profile.default", "default.zip"));

  @Override
  public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
    Optional<StaticDependency> optDep = STATIC_DEPENDENCIES.stream().filter(dep -> dep.matches(dependency)).findFirst();
    if (optDep.isPresent()) {
      StaticDependency staticDep = optDep.get();

      // Load zip file to temporary file for extraction
      InputStream inputStream = StaticRepository.class.getResourceAsStream(staticDep.getResourceFile());
      Path tempZipFile = Files.createTempFile("static-profile-", ".zip");
      Files.copy(inputStream, tempZipFile, StandardCopyOption.REPLACE_EXISTING);

      try (ZipFile zipFile = new ZipFile(tempZipFile.toFile())) {
        // Extract to a desired location
        zipFile.extractAll(targetPath.toString());
      } finally {
        // Delete temp file
        Files.deleteIfExists(tempZipFile);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Optional<Dependency> resolveDependency(String packageName) {
    return STATIC_DEPENDENCIES.stream().filter(dep -> dep.matches(packageName)).map(StaticDependency::getDependency).findFirst();
  }


  @Value
  private static class StaticDependency {

    Dependency dependency;
    String resourceFile;

    public static StaticDependency of(String packageName, String resourceFile) {
      return new StaticDependency(new DependencyImpl(packageName, VERSION, PackageConfigurationImpl.DEFAULT_VARIANT), RESOURCE_DIR + resourceFile);
    }

    public boolean matches(String packageName) {
      return packageName.equalsIgnoreCase(dependency.getName());
    }

    public boolean matches(Dependency dependency) {
      return matches(dependency.getName());
    }

  }

}
