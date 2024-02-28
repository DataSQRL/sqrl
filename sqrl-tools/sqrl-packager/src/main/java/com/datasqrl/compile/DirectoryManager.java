package com.datasqrl.compile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

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
