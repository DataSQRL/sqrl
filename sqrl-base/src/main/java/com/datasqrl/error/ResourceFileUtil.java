/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
