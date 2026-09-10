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
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.config.PackageJson.EngineConfig;
import java.util.ArrayList;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Resolves a view name to the location where the engine creates the view. */
@FunctionalInterface
public interface ViewIdentifierResolver {

  SqlIdentifier resolve(String viewName);

  /**
   * Resolves a view location with an optional parent and child property. When the parent is set,
   * the child defaults to {@code defaultChild}; without a parent, the configured child is used on
   * its own.
   */
  static ViewIdentifierResolver propertyHierarchy(
      EngineConfig engineConfig, String parentProperty, String childProperty, String defaultChild) {
    return viewName -> {
      var names = new ArrayList<String>();
      var parent = engineConfig.getPropertyOptional(parentProperty);

      if (parent.isPresent()) {
        names.add(parent.get());
        names.add(engineConfig.getPropertyOptional(childProperty).orElse(defaultChild));
      } else {
        engineConfig.getPropertyOptional(childProperty).ifPresent(names::add);
      }

      names.add(viewName);

      return new SqlIdentifier(names, SqlParserPos.ZERO);
    };
  }
}
