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
package com.datasqrl.planner;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.server.ResolvedMetadata;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A {@code CREATE NAMESPACE} declaration: a name plus the parameters shared by every function in
 * the namespace. A parameter with metadata is a hidden JWT claim; a parameter without metadata is
 * an external argument exposed on the namespace field (e.g. {@code admin(asTenantId: String!)}).
 */
public record NamespaceDefinition(Name name, List<Param> params) {

  public record Param(String name, RelDataType type, Optional<ResolvedMetadata> metadata) {
    public boolean isExternal() {
      return metadata.isEmpty();
    }
  }

  public Optional<Param> getParam(String paramName) {
    return params.stream().filter(p -> p.name().equalsIgnoreCase(paramName)).findFirst();
  }

  public List<Param> externalParams() {
    return params.stream().filter(Param::isExternal).toList();
  }
}
