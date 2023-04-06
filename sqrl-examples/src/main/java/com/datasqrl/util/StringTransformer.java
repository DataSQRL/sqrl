package com.datasqrl.util;

import java.net.URLEncoder;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class StringTransformer {

  @SneakyThrows
  public static String toURL(String s) {
    String baseReplace = s.trim().toLowerCase().replace(' ','_');
    baseReplace = baseReplace.replaceAll("[^a-zA-Z0-9_]","");
    return URLEncoder.encode(baseReplace, "UTF-8");
  }

  public static String toURL(String first, String... others) {
    return Stream.concat(Stream.of(first), Arrays.stream(others)).map(StringTransformer::toURL)
        .collect(Collectors.joining("/"));
  }

}
