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
package com.datasqrl.lsp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Settings of the language server, passed by the editor as {@code initializationOptions}, and
 * changed with {@code workspace/didChangeConfiguration}.
 *
 * @param packageFiles the package configuration files of every project, relative to the project
 *     root, if they are named the same in all projects; a project is then found by searching
 *     upwards from a script for the first of these files. By default, the package files of the
 *     project of a script are found by their content, see {@link ProjectPackages}.
 * @param checkDelayMillis how long to wait after an edit before checking, so that typing does not
 *     trigger a check for every keystroke
 * @param fullCompileOnSave whether to run the full compilation when a file is saved, which also
 *     validates the API (e.g. GraphQL schemas) but takes longer
 * @param selectedPackageFiles the package files that the user selected for a main script: a base,
 *     when several name it as their main script, e.g. one for local runs and one for tests, and any
 *     of its overlays; given as an object from the URIs of scripts to the URI of a base or an array
 *     of the URIs of a base and its overlays
 */
record LspSettings(
    List<String> packageFiles,
    long checkDelayMillis,
    boolean fullCompileOnSave,
    Map<Path, List<Path>> selectedPackageFiles) {

  static final LspSettings DEFAULT = new LspSettings(List.of(), 300, true, Map.of());

  static LspSettings from(Object initializationOptions) {
    return DEFAULT.with(initializationOptions);
  }

  /** These settings, changed by those in the options. */
  LspSettings with(Object options) {
    if (!(options instanceof JsonObject json)) {
      return this;
    }
    var files = packageFiles;
    if (json.get("packageFiles") instanceof JsonArray array && !array.isEmpty()) {
      var names = new ArrayList<String>();
      array.forEach(e -> names.add(e.getAsString()));
      files = List.copyOf(names);
    }
    var delay = getLong(json.get("checkDelayMillis"), checkDelayMillis);
    var compileOnSave =
        json.has("fullCompileOnSave")
            ? json.get("fullCompileOnSave").getAsBoolean()
            : fullCompileOnSave;
    var selected = selectedPackageFiles;
    if (json.get("selectedPackageFiles") instanceof JsonObject selections) {
      var paths = new HashMap<Path, List<Path>>();
      for (var entry : selections.entrySet()) {
        var script = OpenDocuments.toPath(entry.getKey());
        var uris = new ArrayList<JsonElement>();
        if (entry.getValue() instanceof JsonArray array) {
          array.forEach(uris::add);
        } else {
          uris.add(entry.getValue());
        }
        var packageFiles =
            uris.stream()
                .filter(JsonElement::isJsonPrimitive)
                .flatMap(uri -> OpenDocuments.toPath(uri.getAsString()).stream())
                .toList();
        if (script.isPresent() && !packageFiles.isEmpty()) {
          paths.put(script.get(), packageFiles);
        }
      }
      selected = Map.copyOf(paths);
    }
    return new LspSettings(files, delay, compileOnSave, selected);
  }

  private static long getLong(JsonElement element, long defaultValue) {
    return element != null && element.isJsonPrimitive() ? element.getAsLong() : defaultValue;
  }
}
