package com.datasqrl.packager.repository;

import java.nio.file.Path;

import com.datasqrl.config.PackageConfiguration;

public interface PublishRepository {

    boolean publish(Path zipFile, PackageConfiguration pkgConfig);

}
