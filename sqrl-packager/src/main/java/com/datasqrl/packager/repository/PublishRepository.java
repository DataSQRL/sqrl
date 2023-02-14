package com.datasqrl.packager.repository;

import com.datasqrl.packager.config.Dependency;

import java.nio.file.Path;

public interface PublishRepository {

    void publish(Path zipFile, Dependency dependency);

}
