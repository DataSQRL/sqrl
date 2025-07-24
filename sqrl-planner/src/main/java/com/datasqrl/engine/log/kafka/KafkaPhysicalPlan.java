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
package com.datasqrl.engine.log.kafka;

import com.datasqrl.engine.EnginePhysicalPlan;
import java.util.List;
import java.util.Set;
import lombok.Value;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {

  List<NewTopic> topics;
  Set<String> testRunnerTopics;

  public boolean isEmpty() {
    return topics.isEmpty() && testRunnerTopics.isEmpty();
  }
}
