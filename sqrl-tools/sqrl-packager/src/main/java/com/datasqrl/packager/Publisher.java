package com.datasqrl.packager;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.PackageConfiguration;
import com.datasqrl.packager.repository.PublishRepository;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@AllArgsConstructor
public class Publisher {

    private final ErrorCollector errors;

    public PackageConfiguration publish(Path packageRoot, PublishRepository publishRepo) {
        Preconditions.checkArgument(Files.isDirectory(packageRoot));
        Path packageInfo = packageRoot.resolve(Packager.PACKAGE_FILE_NAME);
        errors.checkFatal(Files.isRegularFile(packageInfo),"Directory does not contain [%s] package configuration file", packageInfo);
        SqrlConfig pkgConfig = SqrlConfigCommons.fromFiles(errors,packageInfo);

        try {
            PackageConfiguration packageConfig = PackageConfiguration.fromRootConfig(pkgConfig);
            Path zipFile = Files.createTempFile(packageRoot, "package", Zipper.ZIP_EXTENSION);
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

    

}
