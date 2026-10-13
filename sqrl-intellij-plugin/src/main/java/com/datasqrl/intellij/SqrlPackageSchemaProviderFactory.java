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

import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.jetbrains.jsonSchema.extension.JsonSchemaFileProvider;
import com.jetbrains.jsonSchema.extension.JsonSchemaProviderFactory;
import com.jetbrains.jsonSchema.extension.SchemaType;
import com.jetbrains.jsonSchema.impl.JsonSchemaVersion;
import java.util.List;

/**
 * Provides the JSON schema of DataSQRL package files, which the compiler validates them with, so
 * that the IDE completes, documents and validates their sections while they are edited.
 */
public final class SqrlPackageSchemaProviderFactory implements JsonSchemaProviderFactory {

  /** The schema, which the build copies from the compiler */
  static final String SCHEMA = "/jsonSchema/packageSchema.json";

  @Override
  public List<JsonSchemaFileProvider> getProviders(Project project) {
    return List.of(new PackageSchemaProvider());
  }

  private static final class PackageSchemaProvider implements JsonSchemaFileProvider {

    @Override
    public boolean isAvailable(VirtualFile file) {
      return SqrlLspClientDescriptor.isDataSqrlPackage(file);
    }

    @Override
    public String getName() {
      return "DataSQRL package";
    }

    @Override
    public VirtualFile getSchemaFile() {
      return JsonSchemaProviderFactory.getResourceFile(
          SqrlPackageSchemaProviderFactory.class, SCHEMA);
    }

    /**
     * IntelliJ applies no schema to a file that several schemas are mapped to, unless one of them
     * is a user schema, e.g. to a DataSQRL {@code package.json}, which the Node.js plugin maps to
     * the schema of npm. This schema is only mapped to DataSQRL package files, so it takes
     * precedence as a user schema.
     */
    @Override
    public SchemaType getSchemaType() {
      return SchemaType.userSchema;
    }

    @Override
    public JsonSchemaVersion getSchemaVersion() {
      return JsonSchemaVersion.SCHEMA_7;
    }
  }
}
