package com.datasqrl.graphql.generate;

public class NameUtil {

  public static String toSafeName(String name) {
    return name.replaceAll("[^_0-9A-Za-z]+", "_");
  }
}
