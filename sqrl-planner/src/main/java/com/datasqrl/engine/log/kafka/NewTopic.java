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
import java.util.List;
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

  /**
   * The name of the table for this mutation or subscription which can differ from the topic name
   */
  private String tableName;

  /** The format as defined in the FlinkSQL connector format option */
  private String format;

  private int numPartitions;
  private short replicationFactor;
  private Type type;

  /**
   * Fields from the message body/value that form the key of a message. Empty if there is no message
   * key
   */
  private List<String> messageKeys;

  /**
   * We make the simplifying assumption that all fields are included in the message body/value which
   * is described by the messageSchema. The schema for the message key can thus be derived by
   * selecting the messageKeys from the messageSchema.
   */
  private String messageSchema;

  /** Additional configuration options that are passed through to the engine */
  private Map<String, String> config;

  public NewTopic(String topicName, String tableName) {
    this(topicName, tableName, null, 1, (short) 3, Type.SUBSCRIPTION, List.of(), "", Map.of());
  }
}
