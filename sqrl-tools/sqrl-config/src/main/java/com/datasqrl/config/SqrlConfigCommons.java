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
package com.datasqrl.config;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import com.datasqrl.error.ErrorCollector;

/** Delegates all configuration loading methods to the Jackson-based implementation. */
public class SqrlConfigCommons {

 
  private SqrlConfigCommons() {}

  public static PackageJson getDefaultPackageJson(ErrorCollector errors) {
    return SqrlConfigJackson.fromFilesPackageJson(errors, List.of());
  }

  public static PackageJson fromFilesPackageJson(ErrorCollector errors, List<Path> files) {
    return SqrlConfigJackson.fromFilesPackageJson(errors, files);
  }

  public static PackageJson fromFilesPublishPackageJson(ErrorCollector errors, List<Path> files) {
    return SqrlConfigJackson.fromFilesPublishPackageJson(errors, files);
  }

  public static SqrlConfig fromFiles(ErrorCollector errors, Path firstFile) {
    return SqrlConfigJackson.fromFiles(errors, firstFile);
  }

  public static boolean validateJsonFile(
      Path jsonFilePath, String schemaResourcePath, ErrorCollector errors) {
    return SqrlConfigJackson.validateJsonFile(jsonFilePath, schemaResourcePath, errors);
  }

  public static SqrlConfig getPackageConfig(
      ErrorCollector errors, String jsonSchemaResource, List<Path> files) {
    return SqrlConfigJackson.getPackageConfig(errors, jsonSchemaResource, files);
  }

  public static SqrlConfig fromURL(ErrorCollector errors, URL url) {
    return SqrlConfigJackson.fromURL(errors, url);
  }

  public static SqrlConfig fromString(ErrorCollector errors, String string) {
    return SqrlConfigJackson.fromString(errors, string);
  }

  public static SqrlConfig create(ErrorCollector errors, int version) {
    return SqrlConfigJackson.create(errors, version);
  }
}
