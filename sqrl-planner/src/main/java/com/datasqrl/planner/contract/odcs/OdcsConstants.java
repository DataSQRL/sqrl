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
package com.datasqrl.planner.contract.odcs;

import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Constants of the Open Data Contract Standard (ODCS) support. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OdcsConstants {

  /** The extension of a data contract file, which sits next to the script of the same name */
  public static final String FILE_EXTENSION = "odcs.yaml";

  /** Extensions that look like a data contract file but are not read */
  public static final List<String> UNSUPPORTED_FILE_EXTENSIONS = List.of("odcs.yml", "odcs.json");

  public static final String KIND = "DataContract";

  /**
   * The {@code apiVersion} of the contracts that the compiler writes. It reads the v3.2 releases:
   * older v3 versions write timestamp, time and map types differently, so a contract of such a
   * version has to be converted first.
   */
  public static final String API_VERSION = "v3.2.0";

  public static final String SUPPORTED_VERSION_PREFIX = "v3.2.";

  /** The contract version of the documents that the compiler generates */
  public static final String GENERATED_VERSION = "1.0.0";

  /** The tag whose value sets the classification of a property */
  public static final String CLASSIFICATION_TAG = "classification";

  /**
   * The JSON schema of ODCS v3.2.0. Copied from {@code schema/odcs-json-schema-v3.2.0.json} at tag
   * {@code v3.2.0} of {@code github.com/bitol-io/open-data-contract-standard} (Apache-2.0) on
   * 2026-11-20.
   */
  public static final String JSON_SCHEMA_RESOURCE = "/jsonSchema/odcs-json-schema-v3.2.0.json";

  /** The fields of a contract that the compiler reads before it maps the contract to its model */
  public static final String API_VERSION_FIELD = "apiVersion";

  public static final String SCHEMA_FIELD = "schema";
  public static final String NAME_FIELD = "name";
  public static final String PROPERTIES_FIELD = "properties";
  public static final String TAGS_FIELD = "tags";
  public static final String CLASSIFICATION_FIELD = "classification";
}
