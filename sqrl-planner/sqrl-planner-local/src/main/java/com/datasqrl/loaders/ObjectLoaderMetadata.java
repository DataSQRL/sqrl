package com.datasqrl.loaders;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import lombok.Value;

@Value
public class ObjectLoaderMetadata {
  List<URI> files;
  Path resourcePath;
  List<String> suffix;
}
