package com.datasqrl.packager.repository;

import com.datasqrl.packager.config.PackageConfiguration;
import java.nio.file.Path;

public interface PublishRepository {

    boolean publish(Path zipFile, PackageConfiguration pkgConfig);

}
