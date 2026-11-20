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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.McpMethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Links the MCP tools of an API version to its MCP apps.
 *
 * <p>A tool is linked to the app that the {@code appName} of its {@code @api} directive names, or
 * else to the app with the tool's name. An empty {@code appName} links no app. Names match
 * regardless of case.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class McpAppLinker {

  /**
   * Returns the operations with the {@code appName} of each tool that is linked to an app set to
   * the name of that app, and cleared for all other operations. Without MCP Apps support, no
   * operation is linked: then an {@code appName} on an operation is an error if the API version
   * serves MCP, and ignored if it doesn't.
   *
   * @param operations the operations of the API version
   * @param apps the apps of the API version, or empty if it has no MCP Apps support
   * @param mcpEnabled whether MCP is one of the API protocols
   * @param apiVersion the API version, for messages
   * @param errors the collector for errors and warnings
   * @return the operations, in the same order
   */
  public static List<ApiOperation> link(
      List<ApiOperation> operations,
      Optional<List<McpAppSource>> apps,
      boolean mcpEnabled,
      String apiVersion,
      ErrorCollector errors) {
    if (apps.isPresent()) {
      return linkToApps(operations, apps.get(), errors);
    }
    if (mcpEnabled) {
      checkWithoutApps(operations, apiVersion, errors);
    }
    return operations.stream().map(operation -> withAppName(operation, null)).toList();
  }

  private static List<ApiOperation> linkToApps(
      List<ApiOperation> operations, List<McpAppSource> apps, ErrorCollector errors) {
    var appNamesByKey = new HashMap<String, String>();
    apps.forEach(app -> appNamesByKey.put(key(app.name()), app.name()));

    var linkedApps = new HashSet<String>();
    var linkedOperations = new ArrayList<ApiOperation>();
    for (var operation : operations) {
      var appName = findApp(operation, appNamesByKey, errors);
      appName.ifPresent(linkedApps::add);
      linkedOperations.add(withAppName(operation, appName.orElse(null)));
    }

    apps.stream()
        .map(McpAppSource::name)
        .filter(name -> !linkedApps.contains(name))
        .forEach(name -> errors.warn("The MCP app '%s' is not linked to any tool", name));
    return linkedOperations;
  }

  /**
   * Checks the operations of an API version that serves MCP without MCP Apps support. A non-empty
   * {@code appName} is an error, since there is no app to link. A tool for apps only gets a
   * warning, since there is no app to call it.
   */
  private static void checkWithoutApps(
      List<ApiOperation> operations, String apiVersion, ErrorCollector errors) {
    for (var operation : operations) {
      errors.checkFatal(
          operation.getAppName() == null || operation.getAppName().isEmpty(),
          "The operation '%s' has appName '%s', but API version '%s' has no MCP apps:"
              + " configure their directory in 'script.api.%s.mcp-apps'",
          operation.getName(),
          operation.getAppName(),
          apiVersion,
          apiVersion);
      if (operation.getMcpMethod() == McpMethodType.APP) {
        errors.warn(
            "The operation '%s' is a tool for MCP apps only (mcp: %s), but API version '%s' has"
                + " no MCP apps that could call it: configure their directory in"
                + " 'script.api.%s.mcp-apps'",
            operation.getName(), McpMethodType.APP, apiVersion, apiVersion);
      }
    }
  }

  /** Returns the name of the app that the operation is linked to, if any. */
  private static Optional<String> findApp(
      ApiOperation operation, Map<String, String> appNamesByKey, ErrorCollector errors) {
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
      return Optional.ofNullable(appNamesByKey.get(key(operation.getName())));
    }
    if (appName.isEmpty()) {
      return Optional.empty();
    }
    var linkedAppName = appNamesByKey.get(key(appName));
    errors.checkFatal(
        linkedAppName != null,
        "The operation '%s' has appName '%s', but there is no MCP app with that name",
        operation.getName(),
        appName);
    return Optional.ofNullable(linkedAppName);
  }

  private static ApiOperation withAppName(ApiOperation operation, String appName) {
    return Objects.equals(operation.getAppName(), appName)
        ? operation
        : operation.toBuilder().appName(appName).build();
  }

  private static String key(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
