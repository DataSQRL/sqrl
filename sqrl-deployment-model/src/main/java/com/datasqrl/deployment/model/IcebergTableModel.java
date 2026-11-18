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
package com.datasqrl.deployment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * An Iceberg table in a deployment file, which the deployment creates and writes to.
 *
 * @param tableName the name of the table in the catalog
 * @param database the catalog database of the table
 * @param catalogType the type of the catalog, e.g. {@code hadoop}; null for a catalog that {@code
 *     catalogImpl} implements
 * @param catalogImpl the class that implements a custom catalog, e.g. AWS Glue; null otherwise
 * @param warehouse the warehouse location, which can reference environment variables; null if the
 *     catalog takes none
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IcebergTableModel(
    String tableName, String database, String catalogType, String catalogImpl, String warehouse) {}
