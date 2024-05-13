package com.datasqrl.packager.repository;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.packager.util.FileHash;
import com.datasqrl.packager.util.GeneratePackageId;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class ValidatePublication implements PublishRepository {

  public static final String PUBLICATION_FILENAME_FORMAT = "package_%d-%s.json";

  private final @NonNull String authorId;

  private final Path outputDir;
  private final @NonNull ErrorCollector errors;

  @Override
  public boolean publish(Path zipFile, PackageConfiguration packageConfig) {
    Preconditions.checkArgument(Files.isRegularFile(zipFile), "Cannot find package zip file: %s", zipFile);
//        Path packageInfo = directory.resolve(Packager.PACKAGE_FILE_NAME);
    validatePackageConfig(packageConfig);
    String uniqueId = GeneratePackageId.generate();
    String file = uniqueId + Zipper.ZIP_EXTENSION;
    String hash;
    try {
      hash = FileHash.getFor(zipFile);
    } catch (IOException ex) {
      throw errors.handle(ex);
    }
    Instant pubTime = Instant.now();
    Publication publication = new Publication(packageConfig, uniqueId, file, hash, authorId, pubTime.toString());

    //Write results if outputdir is configured
    Preconditions.checkArgument(outputDir==null || Files.isDirectory(outputDir), "Output directory does not exist: " + outputDir);
    if (outputDir != null) {
      Path destFile = outputDir.resolve(file);
      Path pkgFile = outputDir.resolve(String.format(PUBLICATION_FILENAME_FORMAT, pubTime.toEpochMilli(), uniqueId));
      try {
        Files.copy(zipFile, destFile);
        new Deserializer().writeJson(pkgFile, publication);
      } catch (IOException ex) {
        throw errors.handle(ex);
      }
    }
    return true;
  }

  public static void validatePackageConfig(PackageConfiguration pkgConfig) {
    pkgConfig.checkInitialized();
    NamePath namePath = NamePath.parse(pkgConfig.getName());
    Preconditions.checkArgument(namePath.size()>=2, "Invalid package name: %s. "
        + "Should have at least two components: name-of-organization.package-name", pkgConfig.getName());
    Preconditions.checkArgument(pkgConfig.getType()!=null && PackageTypes.valueOf(pkgConfig.getType())!=null,
        "Invalid package type: %s", pkgConfig.getType());
  }

}
