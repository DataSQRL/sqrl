package com.datasqrl.packager.repository;

import java.io.IOException;
import java.nio.file.Path;

import com.datasqrl.config.Dependency;

public interface CacheRepository {

    void cacheDependency(Path zipFile, Dependency dependency) throws IOException;

}
