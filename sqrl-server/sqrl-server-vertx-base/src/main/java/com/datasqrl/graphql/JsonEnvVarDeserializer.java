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
package com.datasqrl.graphql;

import com.datasqrl.env.GlobalEnvironmentStore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Custom deserializer to replace environment variables in JSON strings. */
public class JsonEnvVarDeserializer extends JsonDeserializer<String> {

  private Map<String, String> env;

  public JsonEnvVarDeserializer() {
    env = GlobalEnvironmentStore.getAll();
  }

  public JsonEnvVarDeserializer(Map<String, String> env) {
    this.env = env;
  }

  @Override
  public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    var value = p.getText();
    return replaceWithEnv(this.env, value);
  }

  public String replaceWithEnv(Map<String, String> env, String value) {
    var pattern = Pattern.compile("\\$\\{(.+?)\\}");
    var matcher = pattern.matcher(value);
    var result = new StringBuffer();
    while (matcher.find()) {
      var key = matcher.group(1);
      var envVarValue = env.get(key);
      if (envVarValue != null) {
        matcher.appendReplacement(result, Matcher.quoteReplacement(envVarValue));
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }
}
