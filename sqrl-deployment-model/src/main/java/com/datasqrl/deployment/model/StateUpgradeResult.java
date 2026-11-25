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

/**
 * The result of {@link FlinkStateUpgradeCheck#check}.
 *
 * @param breaks the reasons why Flink cannot restore the state, empty when the restore can work
 * @param notes the messages that do not fail the compile, each with its own level
 */
public record StateUpgradeResult(List<StateUpgradeBreak> breaks, List<StateUpgradeNote> notes) {}
