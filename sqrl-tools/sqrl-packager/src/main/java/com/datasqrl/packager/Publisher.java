package com.datasqrl.packager;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.config.PackageConfiguration;
import com.datasqrl.packager.repository.PublishRepository;
import com.datasqrl.packager.util.Zipper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Publisher {

    private final ErrorCollector errors;

    public PackageConfiguration publish(Path packageRoot, PublishRepository publishRepo) {
        Preconditions.checkArgument(Files.isDirectory(packageRoot));
        Path packageInfo = packageRoot.resolve(Packager.PACKAGE_FILE_NAME);
        errors.checkFatal(Files.isRegularFile(packageInfo),"Directory does not contain [%s] package configuration file", packageInfo);

        try {
            PackageConfiguration packageConfig = new Deserializer().mapJsonField(packageInfo, GlobalPackageConfiguration.PACKAGE_NAME, PackageConfiguration.class);
            packageConfig.initialize(errors);
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
