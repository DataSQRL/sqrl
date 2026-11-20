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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;

/**
 * The parts of a build that decide if Flink can restore the state of its job.
 *
 * @param compiledPlan the content of {@code flink-compiled-plan.json}
 * @param maxParallelism the {@code pipeline.max-parallelism} of {@code flink-config.yaml}, empty
 *     when the project does not set it
 */
public record FlinkJobPlan(JsonNode compiledPlan, Optional<Integer> maxParallelism) {}
