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
package com.datasqrl.planner.tables;

import com.datasqrl.planner.dag.plan.MutationQuery;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.flink.table.catalog.ResolvedSchema;

/**
 * Metadata we keep track off for imported/exported tables and their definition
 *
 * @param connectorConfig The connector configuration for the source table
 * @param schema The Flink schema of the source table
 * @param mutationDefinition This is set for internal CREATE TABLE definitions that map to mutations
 *     only, otherwise null It contains the metadata information from the log engine on where to
 *     write the data
 */
public record SourceSinkTableAnalysis(
    @NonNull FlinkConnectorConfig connectorConfig,
    @NonNull ResolvedSchema schema,
    @Nullable MutationQuery mutationDefinition) {}
