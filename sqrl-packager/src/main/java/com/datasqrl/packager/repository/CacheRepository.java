package com.datasqrl.packager.repository;

import com.datasqrl.packager.config.Dependency;

import java.io.IOException;
import java.nio.file.Path;

public interface CacheRepository {

    void cacheDependency(Path zipFile, Dependency dependency) throws IOException;

}
