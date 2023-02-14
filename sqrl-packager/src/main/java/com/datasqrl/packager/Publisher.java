package com.datasqrl.packager;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.config.PackageConfiguration;
import com.datasqrl.packager.repository.PublishRepository;
import com.datasqrl.packager.util.Serializer;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@AllArgsConstructor
public class Publisher {

    private final ErrorCollector errors;

    public Dependency publish(Path packageRoot, PublishRepository publishRepo) {
        Preconditions.checkArgument(Files.isDirectory(packageRoot));
        Path packageInfo = packageRoot.resolve(Packager.PACKAGE_FILE_NAME);
        errors.checkFatal(Files.isRegularFile(packageInfo),"Directory does not contain [%s] package configuration file", packageInfo);

        try {
            PackageConfiguration packageConfig = Serializer.read(packageInfo, GlobalPackageConfiguration.PACKAGE_NAME, PackageConfiguration.class);
            packageConfig.initialize(errors);
            Dependency dependency = Dependency.of(packageConfig);
            Path zipFile = Files.createTempFile(packageRoot, "package", Zipper.ZIP_EXTENSION);
            Zipper.compress(zipFile, packageRoot);
            publishRepo.publish(zipFile, dependency);
            Files.deleteIfExists(zipFile);
            return dependency;
        } catch (IOException e) {
            throw errors.handle(e);
        }
    }

    

}
