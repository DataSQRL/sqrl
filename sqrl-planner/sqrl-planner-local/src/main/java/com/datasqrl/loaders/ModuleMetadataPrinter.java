package com.datasqrl.loaders;

import java.util.stream.Collectors;

public class ModuleMetadataPrinter {

  public static String print(ModuleMetadata meta) {
    StringBuilder builder = new StringBuilder();
    ObjectLoaderMetadata objectLoaderMetadata = meta.getObjectLoaderMetadata();
    builder
        .append("Looking in path: " + objectLoaderMetadata.getResourcePath())
        .append("\nFound files: \n")
        .append(
            objectLoaderMetadata.getFiles().isEmpty() ? "No files found." :
            objectLoaderMetadata.getFiles().stream()
                .map(f->f.toString())
                .collect(Collectors.joining("\n")))
        .append("\nSupported files/suffixes:\n")
        .append(String.join("\n", objectLoaderMetadata.getSuffix()));

    return builder.toString();
  }
}
