/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.server;

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Loads the MCP apps of an API version from its apps directory.
 *
 * <p>An app is a self-contained {@code .html} file directly in the apps directory, named by its
 * file name without the extension.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class McpAppLoader {

  private static final String HTML_EXTENSION = "." + SqrlConstants.HTML_EXTENSION;

  /**
   * A valid app name. The name becomes part of the app's resource URI and of the name of its file
   * in the plan directory.
   */
  private static final Pattern APP_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]*");

  /**
   * A comment, or a script or style element with its content, in which markup is text that loads
   * nothing. Group 1 is the start tag of the element.
   */
  private static final Pattern COMMENT_OR_RAW_TEXT_ELEMENT =
      Pattern.compile(
          "<!--.*?-->|(<(script|style)\\b[^>]*>).*?</\\2\\s*>",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** A {@code src} or {@code href} of a script, link or img element, with or without quotes. */
  private static final Pattern RESOURCE_REFERENCE =
      Pattern.compile(
          "<(?:script|link|img)\\b[^>]*?\\s(?:src|href)\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))",
          Pattern.CASE_INSENSITIVE);

  /**
   * Loads the apps in the directory. Subdirectories and files other than {@code .html} are ignored.
   * Fails if the directory doesn't exist or has no apps, since then no tool can be linked to an
   * app.
   *
   * @param directory the apps directory
   * @param configuredDirectory the directory as configured, for messages
   * @param errors the collector for errors and warnings
   * @return the apps, ordered by name
   */
  public static List<McpAppSource> load(
      Path directory, String configuredDirectory, ErrorCollector errors) {
    var files = Files.isDirectory(directory) ? listHtmlFiles(directory, errors) : List.<Path>of();
    errors.checkFatal(
        !files.isEmpty(),
        "The MCP apps directory '%s' does not exist or has no MCP apps: expected an .html file"
            + " directly in it for each app",
        configuredDirectory);

    var contentsByFileName = new LinkedHashMap<String, String>();
    for (var file : files) {
      contentsByFileName.put(file.getFileName().toString(), readApp(file, errors));
    }
    return toApps(contentsByFileName, configuredDirectory, errors);
  }

  /**
   * Returns the apps of the {@code .html} files, named by their file names without the extension.
   * Fails if a name is not a valid app name, or if two names differ only in case.
   *
   * @param contentsByFileName the content of each file, by file name
   * @param configuredDirectory the directory as configured, for messages
   * @param errors the collector for errors and warnings
   * @return the apps, in the order of the files
   */
  static List<McpAppSource> toApps(
      Map<String, String> contentsByFileName, String configuredDirectory, ErrorCollector errors) {
    var apps = new ArrayList<McpAppSource>();
    var namesByKey = new HashMap<String, String>();
    for (var file : contentsByFileName.entrySet()) {
      var fileName = file.getKey();
      var name = fileName.substring(0, fileName.length() - HTML_EXTENSION.length());
      errors.checkFatal(
          APP_NAME.matcher(name).matches(),
          "The MCP app file '%s' in '%s' does not have a valid app name: before the extension,"
              + " the file name must start with a letter, a digit or '_', and may only contain"
              + " letters, digits, '_', '-' and '.'",
          fileName,
          configuredDirectory);
      var otherName = namesByKey.putIfAbsent(key(name), name);
      errors.checkFatal(
          otherName == null,
          "The MCP apps '%s' and '%s' in '%s' have names that differ only in case",
          otherName,
          name,
          configuredDirectory);

      var content = file.getValue();
      externalReference(content)
          .ifPresent(
              reference ->
                  errors.warn(
                      "The MCP app '%s' loads '%s', so it is not self-contained. Inline all scripts,"
                          + " styles and assets in the app's HTML file.",
                      name, reference));
      apps.add(new McpAppSource(name, content));
    }
    return apps;
  }

  /**
   * Returns the first {@code src} or {@code href} with which a script, link or img element loads a
   * resource from outside the app. This checks the markup only: it ignores what looks like markup
   * in scripts, stylesheets and comments, and it does not see what scripts and stylesheets load.
   */
  static Optional<String> externalReference(String html) {
    var markup = COMMENT_OR_RAW_TEXT_ELEMENT.matcher(html).replaceAll("$1");
    var matcher = RESOURCE_REFERENCE.matcher(markup);
    while (matcher.find()) {
      var reference =
          Stream.of(matcher.group(1), matcher.group(2), matcher.group(3))
              .filter(value -> value != null)
              .findFirst()
              .orElse("")
              .trim();
      if (!reference.regionMatches(true, 0, "data:", 0, "data:".length())) {
        return Optional.of(reference);
      }
    }
    return Optional.empty();
  }

  private static List<Path> listHtmlFiles(Path directory, ErrorCollector errors) {
    try (var files = Files.list(directory)) {
      return files
          .filter(Files::isRegularFile)
          .filter(file -> key(file.getFileName().toString()).endsWith(HTML_EXTENSION))
          .sorted(Comparator.comparing(file -> file.getFileName().toString()))
          .toList();
    } catch (IOException e) {
      throw errors.exception("Could not read the MCP apps directory: %s", e.getMessage());
    }
  }

  private static String readApp(Path file, ErrorCollector errors) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw errors.exception("Could not read the MCP app '%s': %s", file, e.getMessage());
    }
  }

  private static String key(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
