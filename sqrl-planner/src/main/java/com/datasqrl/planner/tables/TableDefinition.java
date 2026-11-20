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

import com.datasqrl.planner.contract.TableContract;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.List;
import lombok.NonNull;

/**
 * What the {@code CREATE TABLE} statement of a source or sink table declares besides its schema and
 * its tags, which the table analysis has. The data contract of the table is generated from it.
 *
 * @param name the name of the table in its statement, without the suffix of an export
 * @param contract the data contract of the table
 * @param partitionKeys the partition columns as declared, since an engine may remove them from the
 *     table, e.g. Kafka uses them as the message key
 * @param documentation the doc string of the table, with the comments of the columns that the doc
 *     string doesn't document
 * @param isTest whether the statement has the test hint
 */
public record TableDefinition(
    @NonNull String name,
    @NonNull TableContract contract,
    @NonNull List<String> partitionKeys,
    @NonNull Documentation documentation,
    boolean isTest) {}
