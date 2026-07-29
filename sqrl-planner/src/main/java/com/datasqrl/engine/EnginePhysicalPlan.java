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
package com.datasqrl.engine;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A jackson serializable object */
public interface EnginePhysicalPlan {

  String SQL_STATEMENT_DELIMITER = ";\n";

  @JsonIgnore
  default List<DeploymentArtifact> getDeploymentArtifacts() {
    return List.of();
  }

  /** Returns the object written to this stage's JSON plan file. */
  @JsonIgnore
  default Object toFileModel() {
    return this;
  }

  record DeploymentArtifact(String fileSuffix, Object content, ArtifactType artifactType) {

    public DeploymentArtifact(String fileSuffix, Object content) {
      this(fileSuffix, content, ArtifactType.fromObject(content));
    }

    @JsonIgnore
    public boolean isEmpty() {
      if (content == null) {
        return true;
      }

      if (content instanceof String str) {
        return str.isBlank();
      }

      return false;
    }

    public static String toSqlString(String... statements) {
      return toSqlString(Arrays.stream(statements));
    }

    public static String toSqlString(Collection<String> statements) {
      return toSqlString(statements.stream());
    }

    public static String toSqlString(Stream<String> statements) {
      return statements
          .filter(statement -> !statement.isBlank())
          .collect(Collectors.joining(SQL_STATEMENT_DELIMITER));
    }
  }

  enum ArtifactType {
    STRING,
    JSON,
    SERIALIZED;

    public static ArtifactType fromObject(Object content) {
      if (content instanceof String) {
        return STRING;
      }
      if (content instanceof Serializable) {
        return SERIALIZED;
      }

      return JSON;
    }
  }
}
