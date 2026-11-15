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
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.McpApp;
import com.datasqrl.server.operation.McpMethodType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
 * Loads the MCP apps of an API version from its apps directory and links them to its MCP tools.
 *
 * <p>An app is a self-contained {@code .html} file directly in the apps directory, named by its
 * file name without the extension. A tool is linked to the app that the {@code appName} of its
 * {@code @api} directive names, or else to the app with the tool's name. An empty {@code appName}
 * links no app. Names match regardless of case.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class McpAppLinker {

  private static final String HTML_EXTENSION = "." + SqrlConstants.HTML_EXTENSION;

  /** A {@code src} or {@code href} of a script, link or img element, with or without quotes. */
  private static final Pattern RESOURCE_REFERENCE =
      Pattern.compile(
          "<(?:script|link|img)\\b[^>]*?\\s(?:src|href)\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))",
          Pattern.CASE_INSENSITIVE);

  /**
   * Loads the apps in the directory. Subdirectories and files other than {@code .html} are ignored.
   *
   * @param directory the apps directory
   * @param configuredDirectory the directory as configured, for messages
   * @param errors the collector for errors and warnings
   * @return the apps, ordered by name
   */
  public static List<McpApp> loadApps(
      Path directory, String configuredDirectory, ErrorCollector errors) {
    errors.checkFatal(
        Files.isDirectory(directory),
        "The MCP apps directory '%s' does not exist",
        configuredDirectory);

    var contentsByFileName = new LinkedHashMap<String, String>();
    for (var file : listHtmlFiles(directory, errors)) {
      contentsByFileName.put(file.getFileName().toString(), readApp(file, errors));
    }
    return toApps(contentsByFileName, configuredDirectory, errors);
  }

  /**
   * Returns the apps of the {@code .html} files, named by their file names without the extension.
   *
   * @param contentsByFileName the content of each file, by file name
   * @param configuredDirectory the directory as configured, for messages
   * @param errors the collector for errors and warnings
   * @return the apps, in the order of the files
   */
  static List<McpApp> toApps(
      Map<String, String> contentsByFileName, String configuredDirectory, ErrorCollector errors) {
    var apps = new ArrayList<McpApp>();
    var namesByKey = new HashMap<String, String>();
    for (var file : contentsByFileName.entrySet()) {
      var name = file.getKey().substring(0, file.getKey().length() - HTML_EXTENSION.length());
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
      apps.add(McpApp.of(name, content));
    }
    return apps;
  }

  /**
   * Links the MCP tools to the apps and returns the operations with the {@code appName} of each
   * linked tool set to the name of its app, and cleared for all other operations.
   *
   * @param operations the operations of the API version
   * @param apps the apps of the API version
   * @param errors the collector for errors and warnings
   * @return the linked operations, in the same order
   */
  public static List<ApiOperation> link(
      List<ApiOperation> operations, List<McpApp> apps, ErrorCollector errors) {
    var appsByKey = new HashMap<String, McpApp>();
    apps.forEach(app -> appsByKey.put(key(app.name()), app));

    var linkedApps = new HashSet<String>();
    var linkedOperations = new ArrayList<ApiOperation>();
    for (var operation : operations) {
      var app = findApp(operation, appsByKey, errors);
      app.ifPresent(found -> linkedApps.add(found.name()));
      linkedOperations.add(
          operation.toBuilder().appName(app.map(McpApp::name).orElse(null)).build());
    }

    apps.stream()
        .filter(app -> !linkedApps.contains(app.name()))
        .forEach(app -> errors.warn("The MCP app '%s' is not linked to any tool", app.name()));
    return linkedOperations;
  }

  /**
   * Returns the operations with no app linked, for an API version without MCP Apps support. A
   * non-empty {@code appName} on an operation is an error, since there is no app to link.
   */
  public static List<ApiOperation> withoutApps(
      List<ApiOperation> operations, String apiVersion, ErrorCollector errors) {
    operations.forEach(
        operation ->
            errors.checkFatal(
                operation.getAppName() == null || operation.getAppName().isEmpty(),
                "The operation '%s' has appName '%s', but API version '%s' has no MCP apps:"
                    + " configure their directory in 'script.api.%s.mcp-apps'",
                operation.getName(),
                operation.getAppName(),
                apiVersion,
                apiVersion));
    return unlinked(operations);
  }

  /** Returns the operations with no app linked. */
  public static List<ApiOperation> unlinked(List<ApiOperation> operations) {
    return operations.stream()
        .map(
            operation ->
                operation.getAppName() == null
                    ? operation
                    : operation.toBuilder().appName(null).build())
        .toList();
  }

  private static Optional<McpApp> findApp(
      ApiOperation operation, Map<String, McpApp> appsByKey, ErrorCollector errors) {
    var appName = operation.getAppName();
    if (!operation.isMcpEndpoint()) {
      errors.checkFatal(
          appName == null || appName.isEmpty(),
          "The operation '%s' has appName '%s' but is not exposed over MCP (mcp: %s)",
          operation.getName(),
          appName,
          McpMethodType.NONE);
      return Optional.empty();
    }
    if (appName == null) {
      return Optional.ofNullable(appsByKey.get(key(operation.getName())));
    }
    if (appName.isEmpty()) {
      return Optional.empty();
    }
    var app = appsByKey.get(key(appName));
    errors.checkFatal(
        app != null,
        "The operation '%s' has appName '%s', but there is no MCP app with that name",
        operation.getName(),
        appName);
    return Optional.ofNullable(app);
  }

  /** Returns the first {@code src} or {@code href} that loads a resource from outside the app. */
  static Optional<String> externalReference(String html) {
    var matcher = RESOURCE_REFERENCE.matcher(html);
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
