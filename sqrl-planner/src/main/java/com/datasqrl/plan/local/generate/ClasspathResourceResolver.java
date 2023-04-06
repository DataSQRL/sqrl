package com.datasqrl.plan.local.generate;

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.loaders.ResourceResolver;
import com.datasqrl.name.NamePath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClasspathResourceResolver implements ResourceResolver {

  public static final String BUILD = "build/";
  private final ArrayListMultimap<String, URI> directories = ArrayListMultimap.create();
  private final Map<String, URI> files = new HashMap<>();
  public ClasspathResourceResolver() {
    try {
      ClassLoader classLoader = getClass().getClassLoader();
      ClassPath classPath = ClassPath.from(classLoader);

      for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
        String resourceName = resourceInfo.getResourceName();
        if (resourceName.startsWith(BUILD)) {
          URI resourceURI = Resources.getResource(resourceName).toURI();
          String nameWithoutBuild = resourceName.substring(BUILD.length());
          directories.put(getDirectoryName(nameWithoutBuild), resourceURI);
          files.put(nameWithoutBuild, resourceURI);
        }
      }
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Failed to enumerate classpath resources", e);
    }
  }

  private String getDirectoryName(String resourceName) {
    //Root directory is empty string ("").
    if (!resourceName.contains("/")) {
      return "";
    }
    return resourceName.substring(0,
        resourceName.lastIndexOf("/"));
  }

  private String getFileName(String resourceName) {
    return resourceName;
  }

  @Override
  public List<URI> loadPath(NamePath namePath) {
    System.out.println(directories);
    Path path = namepath2Path(Path.of(""), namePath);
    return directories.get(path.toString());
  }

  @Override
  public Optional<URI> resolveFile(NamePath namePath) {
    Path path = namepath2Path(Path.of(""), namePath);
    System.out.println(files);
    System.out.println(path.toString());

    return Optional.ofNullable(files.get(path.toString()));
  }

  @Override
  public String toString() {
    return "ClasspathResourceResolver";
  }

}