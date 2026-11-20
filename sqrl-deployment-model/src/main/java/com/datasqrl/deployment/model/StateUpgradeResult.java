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

import java.util.List;
import java.util.Optional;

/**
 * The result of {@link FlinkStateUpgradeCheck#check}.
 *
 * @param breaks the reasons why Flink cannot restore the state, empty when the restore can work
 * @param warnings the risks that the check cannot decide, for example a {@code
 *     pipeline.max-parallelism} that only one build sets
 * @param flinkVersionChange {@code "<old> -> <new>"} when the Flink versions are different. This is
 *     information only.
 */
public record StateUpgradeResult(
    List<StateUpgradeBreak> breaks, List<String> warnings, Optional<String> flinkVersionChange) {}
