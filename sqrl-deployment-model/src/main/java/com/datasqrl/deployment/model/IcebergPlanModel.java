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
import java.util.List;

/**
 * The tables of the {@code iceberg.json} deployment file. The file also has the plans of the
 * Iceberg table format and its query engines, which this model leaves out.
 *
 * @param tables the Iceberg tables that the deployment creates and writes to
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IcebergPlanModel(List<IcebergTableModel> tables) {

  public IcebergPlanModel {
    tables = tables == null ? List.of() : List.copyOf(tables);
  }
}
