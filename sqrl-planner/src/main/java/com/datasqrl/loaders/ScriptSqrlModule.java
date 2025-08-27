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
package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.MainScript;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final String scriptContent;
  private final Path scriptPath;
  private final NamePath namePath;
  private final ModuleLoader moduleLoader;

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    throw new UnsupportedOperationException(
        "Cannot import an individual table from SQRL script. "
            + "Use `%s;` to import entire script in a separate database or `%s.*;` to import script inline."
                .formatted(namePath, namePath));
  }

  // Planning all tables. Return a single planning ns object and add all tables
  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return List.of(new ScriptNamespaceObject(true));
  }

  public NamespaceObject asNamespaceObject() {
    return new ScriptNamespaceObject(false);
  }

  @AllArgsConstructor
  public class ScriptNamespaceObject implements NamespaceObject {

    @Getter boolean inline;

    @Override
    public Name name() {
      return namePath.getLast();
    }

    public ModuleLoader getModuleLoader() {
      return moduleLoader;
    }

    public MainScript getScript() {
      return new MainScript() {
        @Override
        public Optional<Path> getPath() {
          return Optional.of(scriptPath);
        }

        @Override
        public String getContent() {
          return scriptContent;
        }
      };
    }
  }
}
