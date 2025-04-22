package com.datasqrl.graphql;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * Custom deserializer to replace environment variables in JSON strings.
 */
public class JsonEnvVarDeserializer extends JsonDeserializer<String> {

  private Map<String, String> env;

  public JsonEnvVarDeserializer() {
    env = System.getenv();
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