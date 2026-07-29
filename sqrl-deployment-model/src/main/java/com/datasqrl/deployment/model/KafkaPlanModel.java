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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** The contents of a Kafka deployment file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KafkaPlanModel(
    List<KafkaNewTopicModel> topics, List<KafkaNewTopicModel> testRunnerTopics) {

  public KafkaPlanModel {
    topics = topics == null ? List.of() : List.copyOf(topics);
    testRunnerTopics = testRunnerTopics == null ? List.of() : List.copyOf(testRunnerTopics);
  }

  @JsonIgnore
  public boolean isEmpty() {
    return topics.isEmpty() && testRunnerTopics.isEmpty();
  }
}
