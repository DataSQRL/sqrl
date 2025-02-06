package com.datasqrl.packager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.datasqrl.config.PackageConfiguration;
import com.datasqrl.config.PackageConfigurationImpl;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.repository.PublishRepository;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class Publisher {

    private static final String README_NAME = "README.md";

    private final ErrorCollector errors;

    public PackageConfiguration publish(Path packageRoot, PublishRepository publishRepo) {
        Preconditions.checkArgument(Files.isDirectory(packageRoot));
        var packageInfo = packageRoot.resolve(Packager.PACKAGE_JSON);
        errors.checkFatal(Files.isRegularFile(packageInfo),"Directory does not contain [%s] package configuration file", packageInfo);
        var pkgConfig = SqrlConfigCommons.fromFilesPublishPackageJson(errors, List.of(packageInfo));

        try {
            var packageConfig = (PackageConfigurationImpl) pkgConfig.getPackageConfig();
            addReadme(packageRoot, packageConfig);

            var zipFile = Files.createTempFile(packageRoot, "package", Zipper.ZIP_EXTENSION);
            try {
                Zipper.compress(zipFile, packageRoot);
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
