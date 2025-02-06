package com.datasqrl.compile;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectoryManager {

    @SneakyThrows
    public static void prepareTargetDirectory(Path targetDir) {
        if (Files.isDirectory(targetDir)) {
            FileUtils.cleanDirectory(targetDir.toFile());
        } else {
            Files.createDirectories(targetDir);
        }
    }
}
