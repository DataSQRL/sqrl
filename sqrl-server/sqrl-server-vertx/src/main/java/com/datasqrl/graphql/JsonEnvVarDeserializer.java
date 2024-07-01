package com.datasqrl.graphql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonEnvVarDeserializer extends JsonDeserializer<String> {

  @Override
  public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String value = p.getText();
    return replaceWithEnv(System.getenv(), value);
  }

  public String replaceWithEnv(Map<String, String> env, String value) {
    Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
    Matcher matcher = pattern.matcher(value);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String key = matcher.group(1);
      String envVarValue = env.get(key);
      if (envVarValue != null) {
        matcher.appendReplacement(result, Matcher.quoteReplacement(envVarValue));
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }
}