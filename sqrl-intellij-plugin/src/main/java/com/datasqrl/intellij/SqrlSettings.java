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

import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.components.PersistentStateComponent;
import com.intellij.openapi.components.Service;
import com.intellij.openapi.components.State;
import com.intellij.openapi.components.Storage;
import java.io.IOException;
import java.util.Properties;

/** How the IDE starts the SQRL language server, for all projects. */
@Service(Service.Level.APP)
@State(name = "DataSqrlSettings", storages = @Storage("datasqrl.xml"))
public final class SqrlSettings implements PersistentStateComponent<SqrlSettings.Options> {

  /** How the language server is started */
  public enum Launcher {
    /** In the Docker image of the CLI */
    DOCKER,
    /** From the jar of the CLI, e.g. one that was built locally */
    JAR
  }

  /** The settings, which are public fields to be stored by the IDE. */
  public static final class Options {
    public Launcher launcher = Launcher.DOCKER;
    public String dockerImage = defaultDockerImage();
    public String jarPath = "";

    /** The Java executable that runs the jar, or empty for the one of JAVA_HOME or the IDE */
    public String javaPath = "";
  }

  private Options options = new Options();

  /** What the build of the plugin knows, see {@code build.gradle.kts} */
  private static final String BUILD_PROPERTIES = "/com/datasqrl/intellij/build.properties";

  /** The image of the release of the CLI that this plugin was built for. */
  static String defaultDockerImage() {
    var build = new Properties();
    try (var in = SqrlSettings.class.getResourceAsStream(BUILD_PROPERTIES)) {
      if (in != null) {
        build.load(in);
      }
    } catch (IOException e) {
      // The image of the latest release then
    }
    return defaultDockerImage(build);
  }

  /** The image of the release of a build, or the latest one for a development build. */
  static String defaultDockerImage(Properties build) {
    return build.getProperty("image", "datasqrl/cmd:latest");
  }

  public static SqrlSettings getInstance() {
    return ApplicationManager.getApplication().getService(SqrlSettings.class);
  }

  @Override
  public Options getState() {
    return options;
  }

  @Override
  public void loadState(Options state) {
    this.options = state;
  }
}
