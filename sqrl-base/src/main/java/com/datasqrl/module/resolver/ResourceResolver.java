package com.datasqrl.module.resolver;

import com.datasqrl.canonicalizer.NamePath;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Optional;

public interface ResourceResolver {

  List<URI> loadPath(NamePath namePath);

  Optional<URI> resolveFile(NamePath namePath);

  static URL toURL(URI uri) {
    try {
      return uri.toURL();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String getFileName(URI uri) {
    String[] pathSegments = uri.getPath().split("/");
    return pathSegments[pathSegments.length - 1];
  }

}
