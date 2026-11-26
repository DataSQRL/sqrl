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
package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/**
 * The rule for the paths that a project configures in its package file, such as {@code script.main}
 * or {@code script.include.*.package}: a path is relative to the project root and stays inside the
 * workspace, the directory that the compiler runs in. Everything in the workspace is considered
 * safe to read, including other projects, so a path may lead out of the project.
 *
 * <p>Symbolic links count with their targets: a link inside the workspace that points outside it
 * leads outside.
 *
 * @param workspaceDir the workspace directory, which holds the project
 * @param projectRoot the root directory of the project
 */
public record ProjectPaths(Path workspaceDir, Path projectRoot) {

  /** Explains the rule in error messages. */
  public static final String RULE =
      "A path must be relative to the project root and stay inside the workspace.";

  /**
   * Resolves a configured path against the project root, and fails naming the key and the value if
   * the path does not follow the rule.
   *
   * @param key the configuration key, for the error message
   * @param path the path as configured
   * @param errors the collector to fail with
   * @return the normalized path
   */
  public Path resolve(String key, String path, ErrorCollector errors) {
    var resolved = isRelative(path) ? projectRoot.resolve(path) : null;
    // Throws also with a collector that does not abort on fatal errors, since callers use the path
    if (resolved == null || !isInsideWorkspace(resolved)) {
      throw errors.exception("Value [%s] for key [%s] is not valid. %s", path, key, RULE);
    }

    return resolved.normalize();
  }

  /**
   * Checks every path that the package file configures against the rule, also a path that no
   * enabled feature reads, such as the MCP apps of an API version without MCP, so that the rule
   * does not depend on the enabled features. The compiler checks each path again where it reads it.
   *
   * @param config the package configuration
   * @param errors the collector to fail with
   */
  public void validate(PackageJson config, ErrorCollector errors) {
    var script = config.getScriptConfig();
    resolve("script.main", script.getMainScript(), errors);
    script.getGraphql().ifPresent(path -> resolve("script.graphql", path, errors));
    script.getOperations().forEach(path -> resolve("script.operations", path, errors));
    script.getDatabase().ifPresent(path -> resolve("script.database", path, errors));
    for (var api : script.getScriptApiConfigs()) {
      var prefix = "script.api." + api.getVersion() + ".";
      api.getSchema().ifPresent(path -> resolve(prefix + "schema", path, errors));
      api.getOperations().forEach(path -> resolve(prefix + "operations", path, errors));
      api.getOpenApi().ifPresent(path -> resolve(prefix + "openapi", path, errors));
      api.getMcpApps().ifPresent(path -> resolve(prefix + "mcp-apps", path, errors));
    }
    for (var include : script.getIncludeConfigs()) {
      var key = "script.include." + include.getNamespace() + ".package";
      resolve(key, include.getPackage(), errors);
    }
    var testConfig = config.getTestConfig();
    testConfig.getSnapshotDir(this);
    testConfig.getTestDir(this);
  }

  /**
   * Returns whether the path is inside the workspace once symbolic links are resolved. For a path
   * that does not exist, the nearest directory above it that exists is resolved, so a link above a
   * folder that is created later counts as well.
   *
   * <p>A {@code ..} after a link differs between the path as written, in which the OS resolves it
   * from the link's target, and the normalized path, from which it removes the link. Callers use
   * either form, so both must be inside.
   *
   * @param path a path to check, absolute or relative to the working directory
   * @return {@code true} if the real path is inside the real workspace directory
   */
  public boolean isInsideWorkspace(Path path) {
    var workspace = realPath(workspaceDir);
    return realPath(path).startsWith(workspace) && realPath(path.normalize()).startsWith(workspace);
  }

  /**
   * Returns whether the path is relative. A path with a root but no drive, such as {@code \dir} on
   * Windows, is not absolute but not relative either.
   */
  static boolean isRelative(String path) {
    try {
      var p = Path.of(path);
      return p.getRoot() == null && !p.isAbsolute();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  /**
   * Returns whether the path is relative and, once normalized, does not leave the directory that it
   * is resolved against, such as {@code api/../schema.graphqls}.
   */
  public static boolean staysInside(String path) {
    return isRelative(path) && !Path.of(path).normalize().startsWith("..");
  }

  /** Resolves the path the way the OS does, so a {@code ..} after a link applies to its target. */
  private static Path realPath(Path path) {
    var absolute = path.toAbsolutePath();
    var existing = absolute;
    while (existing != null && !Files.exists(existing)) {
      existing = existing.getParent();
    }
    if (existing == null) {
      return absolute.normalize();
    }
    try {
      return existing.toRealPath().resolve(existing.relativize(absolute)).normalize();
    } catch (IOException e) {
      return absolute.normalize();
    }
  }
}
