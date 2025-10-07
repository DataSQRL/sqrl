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

import com.datasqrl.engine.database.EngineCreateTable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class NewTopic implements EngineCreateTable {

  public enum Type {
    MUTATION,
    SUBSCRIPTION
  }

  private String topicName;
  private String tableName;
  private String format;
  private int numPartitions;
  private short replicationFactor;
  private Type type;
  private Map<String, String> config;

  public NewTopic(String topicName, String tableName) {
    this(topicName, tableName, null, Type.SUBSCRIPTION, Map.of());
  }

  public NewTopic(
      String topicName, String tableName, String format, Type type, Map<String, String> config) {
    this(topicName, tableName, format, 1, (short) 3, type, config);
  }
}
