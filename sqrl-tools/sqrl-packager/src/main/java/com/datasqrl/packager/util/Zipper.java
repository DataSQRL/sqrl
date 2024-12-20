package com.datasqrl.packager.util;

import com.datasqrl.util.FileUtil;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ExcludeFileFilter;
import net.lingala.zip4j.model.ZipParameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class Zipper {

    public static final String ZIP_EXTENSION = ".zip";

    public static void compress(Path zipFile, Path directory) throws IOException {
        Files.deleteIfExists(zipFile);
        ExcludeFileFilter excludeFilter = file -> file.getName().endsWith(ZIP_EXTENSION);
        ZipParameters zipParameters = new ZipParameters();
        zipParameters.setExcludeFileFilter(excludeFilter);
        try(ZipFile zip = new ZipFile(zipFile.toFile());) {
	        for (Path p : Files.list(directory).collect(Collectors.toList())) {
	            if (Files.isRegularFile(p) && !FileUtil.isExtension(p, ZIP_EXTENSION)) zip.addFile(p.toFile());
	            else if (Files.isDirectory(p)) zip.addFolder(p.toFile(), zipParameters);
	        }
        }
    }

}

