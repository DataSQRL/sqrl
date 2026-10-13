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
package com.datasqrl.intellij;

import com.intellij.execution.ExecutionException;
import com.intellij.execution.configurations.GeneralCommandLine;
import com.intellij.execution.configurations.PathEnvironmentVariableUtil;
import com.intellij.openapi.application.PathManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.SystemInfo;
import com.intellij.openapi.vfs.VfsUtilCore;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.platform.lsp.api.ProjectWideLspClientDescriptor;
import com.intellij.platform.lsp.api.customization.LspCommandsCustomizer;
import com.intellij.platform.lsp.api.customization.LspCustomization;
import com.intellij.platform.lsp.api.customization.LspDiagnosticsCustomizer;
import com.intellij.platform.lsp.api.customization.LspDiagnosticsSupport;
import com.intellij.util.EnvironmentUtil;
import com.intellij.xml.util.XmlStringUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.Diagnostic;

/** Starts the SQRL language server for the {@code .sqrl} scripts of a project. */
final class SqrlLspClientDescriptor extends ProjectWideLspClientDescriptor {

  static final String NAME = "DataSQRL";
  private static final String SETTINGS = "Settings | Languages & Frameworks | DataSQRL";

  private static final Pattern DATASQRL_SECTION =
      Pattern.compile("\"(script|enabled-engines|connectors|compiler|test-runner)\"\\s*:");

  private static final Pattern NPM_SECTION =
      Pattern.compile(
          "\"(dependencies|devDependencies|peerDependencies|scripts|workspaces)\"\\s*:");

  private static final long MAX_PACKAGE_FILE = 1 << 20;

  SqrlLspClientDescriptor(Project project) {
    super(project, NAME);
  }

  static boolean isSqrl(VirtualFile file) {
    return "sqrl".equals(file.getExtension());
  }

  /**
   * Scripts, and the files of projects whose errors the server reports: package files, which have
   * "package" in their name, and GraphQL schemas and operations.
   */
  @Override
  public boolean isSupportedFile(VirtualFile file) {
    return isSqrl(file) || isPackageFile(file) || isGraphQl(file);
  }

  private static boolean isPackageFile(VirtualFile file) {
    return "json".equals(file.getExtension())
        && file.getName().toLowerCase(Locale.ROOT).contains("package");
  }

  /**
   * Whether a file is a package file of DataSQRL: one that has sections of it, unlike e.g. the
   * package.json of npm, whose {@code engines} section is left out for that reason, or a package
   * file next to scripts that has no sections of npm, e.g. a new one.
   */
  static boolean isDataSqrlPackage(VirtualFile file) {
    if (!isPackageFile(file) || file.getLength() > MAX_PACKAGE_FILE) {
      return false;
    }
    try {
      var text = VfsUtilCore.loadText(file);
      if (DATASQRL_SECTION.matcher(text).find()) {
        return true;
      }
      var dir = file.getParent();
      return !NPM_SECTION.matcher(text).find()
          && dir != null
          && Arrays.stream(dir.getChildren()).anyMatch(SqrlLspClientDescriptor::isSqrl);
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean isGraphQl(VirtualFile file) {
    return "graphqls".equals(file.getExtension()) || "graphql".equals(file.getExtension());
  }

  @Override
  public String getLanguageId(VirtualFile file) {
    if (isPackageFile(file)) {
      return "json";
    }
    return isGraphQl(file) ? "graphql" : "sqrl";
  }

  /** The settings of the server: the package files that the user selected for scripts. */
  @Override
  public Object createInitializationOptions() {
    return SqrlPackageSelection.getInstance(getProject()).settings();
  }

  @Override
  public LspCustomization getLspCustomization() {
    return new LspCustomization() {
      // Runs the tests of code lenses in the IDE
      @Override
      public LspCommandsCustomizer getCommandsCustomizer() {
        return new SqrlTestRunner();
      }

      @Override
      public LspDiagnosticsCustomizer getDiagnosticsCustomizer() {
        return new LspDiagnosticsSupport() {
          // Tooltips are HTML, but messages are text, e.g. with types like <INTEGER>
          @Override
          public String getTooltip(Diagnostic diagnostic) {
            return XmlStringUtil.wrapInHtml(
                XmlStringUtil.escapeString(getMessage(diagnostic)).replace("\n", "<br>"));
          }
        };
      }
    };
  }

  @Override
  public GeneralCommandLine createCommandLine() throws ExecutionException {
    var settings = SqrlSettings.getInstance().getState();
    var command =
        switch (settings.launcher) {
          case DOCKER -> {
            checkServerInDocker(SystemInfo.isWindows);
            yield ServerCommand.docker(docker(), settings.dockerImage.strip(), projectDir());
          }
          case JAR -> ServerCommand.jar(java(settings), jar(settings));
        };
    // Find the executables on the PATH of the user's shell, which the IDE may not have
    return new GeneralCommandLine(command)
        .withParentEnvironmentType(GeneralCommandLine.ParentEnvironmentType.CONSOLE)
        .withWorkDirectory(workDirectory().toFile())
        .withCharset(StandardCharsets.UTF_8);
  }

  /**
   * The working directory of the server, a directory of the IDE: files that checks create at
   * relative paths, e.g. a local Iceberg warehouse, end up there instead of in a project.
   */
  static Path workDirectory() throws ExecutionException {
    var dir = Path.of(PathManager.getSystemPath(), "sqrl-lsp");
    try {
      return Files.createDirectories(dir);
    } catch (IOException e) {
      throw new ExecutionException("Could not create the directory " + dir, e);
    }
  }

  /** The directory of the project, which is mounted into the Docker container. */
  private String projectDir() throws ExecutionException {
    var projectDir = getProject().getBasePath();
    if (projectDir == null || !Files.isDirectory(Path.of(projectDir))) {
      throw new ExecutionException("The project has no directory to mount into Docker");
    }
    return projectDir;
  }

  /**
   * Fails on Windows, where the server cannot run in Docker yet: it would see the files of the
   * project at other paths than the IDE, e.g. not at {@code C:\...}.
   */
  static void checkServerInDocker(boolean windows) throws ExecutionException {
    if (windows) {
      throw new ExecutionException(
          "On Windows, the language server cannot run in Docker yet. Start it from the jar of the"
              + " CLI in "
              + SETTINGS
              + ".");
    }
  }

  static String dockerExecutable(boolean windows) {
    return windows ? "docker.exe" : "docker";
  }

  static String docker() throws ExecutionException {
    var docker =
        PathEnvironmentVariableUtil.findInPath(
            dockerExecutable(SystemInfo.isWindows), EnvironmentUtil.getValue("PATH"), null);
    if (docker == null) {
      throw new ExecutionException(
          "Docker was not found. Install Docker, or start the language server from the jar of the"
              + " CLI in "
              + SETTINGS
              + ".");
    }
    return docker.getPath();
  }

  /**
   * The configured Java, else the one of {@code JAVA_HOME} of the user's shell, else the runtime of
   * the IDE, which may be too new for the CLI: Hadoop, which e.g. Iceberg uses, fails on Java 23
   * and later.
   */
  static String java(SqrlSettings.Options settings) {
    if (!settings.javaPath.isBlank()) {
      return settings.javaPath.strip();
    }
    var javaHome = EnvironmentUtil.getValue("JAVA_HOME");
    if (javaHome != null && !javaHome.isBlank()) {
      for (var name : List.of("java", "java.exe")) {
        var java = Path.of(javaHome.strip(), "bin", name);
        if (Files.isRegularFile(java)) {
          return java.toString();
        }
      }
    }
    return Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  static String jar(SqrlSettings.Options settings) throws ExecutionException {
    var jar = settings.jarPath.strip();
    if (jar.isEmpty() || !Files.isRegularFile(Path.of(jar))) {
      throw new ExecutionException(
          "Set the path of sqrl-cli.jar in "
              + SETTINGS
              + (jar.isEmpty() ? "." : ", " + jar + " does not exist."));
    }
    return new File(jar).getAbsolutePath();
  }
}
