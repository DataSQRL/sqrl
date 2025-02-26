package com.datasqrl.error;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ResourceFileUtil {

  public static String readResourceFileContents(String resourcePath) {
    StringBuilder contentBuilder = new StringBuilder();

    try (InputStream inputStream = ResourceFileUtil.class.getResourceAsStream(resourcePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String currentLine;
      while ((currentLine = reader.readLine()) != null) {
        contentBuilder.append(currentLine).append("\n");
      }
    } catch (IOException | NullPointerException e) {
      throw new IllegalArgumentException("Unable to read resource file: " + resourcePath, e);
    }
    return contentBuilder.toString();
  }

  public static void main(String[] args) {
    String resourcePath = "/errorCodes/cannot_resolve_tablesink.md";
    String fileContent = readResourceFileContents(resourcePath);
    System.out.println(fileContent);
  }
}
