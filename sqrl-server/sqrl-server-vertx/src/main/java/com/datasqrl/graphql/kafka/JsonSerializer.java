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
package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.SqrlObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer implements Serializer<Map> {
  private String encoding;

  public JsonSerializer() {
    this.encoding = StandardCharsets.UTF_8.name();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    var propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) {
      encodingValue = configs.get("serializer.encoding");
    }

    if (encodingValue instanceof String string) {
      this.encoding = string;
    }
  }

  @Override
  public byte[] serialize(String topic, Map data) {
    try {
      return data == null
          ? null
          : SqrlObjectMapper.MAPPER.writeValueAsString(data).getBytes(this.encoding);
    } catch (UnsupportedEncodingException var4) {
      throw new SerializationException(
          "Error when serializing string to byte[] due to unsupported encoding " + this.encoding);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
