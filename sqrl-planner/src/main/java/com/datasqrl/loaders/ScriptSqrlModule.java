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
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final String scriptContent;
  private final Path scriptPath;
  private final NamePath namePath;

  private final AtomicBoolean planned = new AtomicBoolean(false);

  // Planning a single table. Return a planning ns object and then only add that table
  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.of(new ScriptNamespaceObject(Optional.of(name)));
  }

  // Planning all tables. Return a single planning ns object and add all tables
  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return List.of(new ScriptNamespaceObject(Optional.empty()));
  }

  @AllArgsConstructor
  public class ScriptNamespaceObject implements NamespaceObject {

    @Getter Optional<Name> tableName; // specific table name to import, empty for all

    @Override
    public Name getName() {
      return namePath.getLast();
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
