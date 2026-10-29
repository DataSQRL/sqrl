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
package com.datasqrl.eclipse;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.IPreferenceStore;
import org.eclipse.ui.preferences.ScopedPreferenceStore;

/** The preferences of how the language server is started, and the settings that it is given. */
public final class SqrlPreferences extends AbstractPreferenceInitializer {

  static final String BUNDLE = "com.datasqrl.eclipse";

  static final String LAUNCHER = "launcher";
  static final String DOCKER = "docker";
  static final String DOCKER_IMAGE = "dockerImage";
  static final String JAR = "jar";
  static final String JAVA = "java";
  static final String PACKAGE_FILES = "packageFiles";
  static final String CHECK_DELAY_MILLIS = "checkDelayMillis";
  static final String FULL_COMPILE_ON_SAVE = "fullCompileOnSave";

  static final String LAUNCHER_DOCKER = "docker";
  static final String LAUNCHER_JAR = "jar";

  private static ScopedPreferenceStore store;

  static synchronized IPreferenceStore store() {
    if (store == null) {
      store = new ScopedPreferenceStore(InstanceScope.INSTANCE, BUNDLE);
    }
    return store;
  }

  @Override
  public void initializeDefaultPreferences() {
    var defaults = store();
    defaults.setDefault(LAUNCHER, LAUNCHER_DOCKER);
    defaults.setDefault(DOCKER, "docker");
    defaults.setDefault(DOCKER_IMAGE, "datasqrl/cmd:latest");
    defaults.setDefault(JAR, "");
    defaults.setDefault(JAVA, "");
    defaults.setDefault(PACKAGE_FILES, "");
    defaults.setDefault(CHECK_DELAY_MILLIS, 300);
    defaults.setDefault(FULL_COMPILE_ON_SAVE, true);
  }

  /** The settings of the server, which it gets as its initialization options. */
  static Map<String, Object> settings(IPreferenceStore preferences) {
    var settings = new LinkedHashMap<String, Object>();
    var packageFiles = packageFiles(preferences.getString(PACKAGE_FILES));
    if (!packageFiles.isEmpty()) {
      settings.put(PACKAGE_FILES, packageFiles);
    }
    settings.put(CHECK_DELAY_MILLIS, preferences.getInt(CHECK_DELAY_MILLIS));
    settings.put(FULL_COMPILE_ON_SAVE, preferences.getBoolean(FULL_COMPILE_ON_SAVE));
    return settings;
  }

  /** The package files, separated by commas, e.g. {@code package.json, package-test.json}. */
  static List<String> packageFiles(String names) {
    return Arrays.stream(names.split(",")).map(String::strip).filter(name -> !name.isEmpty()).toList();
  }
}
