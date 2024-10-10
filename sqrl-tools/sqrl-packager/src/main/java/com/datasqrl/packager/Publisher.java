package com.datasqrl.packager;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.config.PackageConfigurationImpl;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.repository.PublishRepository;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

@AllArgsConstructor
public class Publisher {

    private static final String README_NAME = "README.md";

    private final ErrorCollector errors;

    public PackageConfiguration publish(Path packageRoot, PublishRepository publishRepo) {
        Preconditions.checkArgument(Files.isDirectory(packageRoot));
        Path packageInfo = packageRoot.resolve(Packager.PACKAGE_JSON);
        errors.checkFatal(Files.isRegularFile(packageInfo),"Directory does not contain [%s] package configuration file", packageInfo);
        PackageJson pkgConfig = SqrlConfigCommons.fromFilesPublishPackageJson(errors, List.of(packageInfo));

        try {
            PackageConfigurationImpl packageConfig = (PackageConfigurationImpl) pkgConfig.getPackageConfig();
            addReadme(packageRoot, packageConfig);

            Path[] sources = packageConfig.getSources().stream().map(Path::of).toArray(Path[]::new);

            Path zipFile = Files.createTempFile(packageRoot, "package", Zipper.ZIP_EXTENSION);
            try {
                Zipper.compress(zipFile, packageRoot, sources);
                if (publishRepo.publish(zipFile, packageConfig)) {
                    return packageConfig;
                } else {
                    return null;
                }
            } finally {
                Files.deleteIfExists(zipFile);
            }
        } catch (IOException e) {
            throw errors.handle(e);
        }
    }

    @SneakyThrows
    private void addReadme(Path packageRoot, PackageConfigurationImpl packageConfig) {
        Path readmePath = packageRoot.resolve(README_NAME);
        if (packageConfig.getReadme() == null && Files.exists(readmePath)) {
            String readmeContent = Files.readString(readmePath);
            packageConfig.setReadme(readmeContent);
        }
    }

}
