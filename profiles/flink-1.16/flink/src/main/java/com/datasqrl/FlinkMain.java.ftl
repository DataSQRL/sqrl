package com.datasqrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkMain {

  public static void main(String[] args) throws Exception {
    new FlinkMain().run();
  }

  public TableResult run() throws Exception {
    Map<String, String> flinkConfig = new HashMap();
<#if config["values"]?? && config["values"]["flink-config"]??>
<#list config["values"]["flink-config"] as key, value>
<#if key?contains(".")>
    flinkConfig.put("${key}", "${value}");
</#if>
</#list>
</#if>
    Configuration sEnvConfig = Configuration.fromMap(flinkConfig);
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
        sEnvConfig);
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(flinkConfig)).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;
    String[] statements = readResourceFile("flink.sql").split("\n\n");
    for (String statement : statements) {
      if (statement.trim().isEmpty()) continue;
      tableResult = tEnv.executeSql(replaceWithEnv(statement));
    }
    return tableResult;
  }

  public String replaceWithEnv(String command) {
      Map<String, String> envVariables = System.getenv();
      Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");

      String substitutedStr = command;
      StringBuffer result = new StringBuffer();
      // First pass to replace environment variables
      Matcher matcher = pattern.matcher(substitutedStr);
      while (matcher.find()) {
        String key = matcher.group(1);
        String envValue = envVariables.getOrDefault(key, "");
        matcher.appendReplacement(result, envValue);;
      }
      matcher.appendTail(result);

      return result.toString();
    }

    private static String readResourceFile(String fileName) {
      try (var inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      } catch (IOException | NullPointerException e) {
        System.err.println("Error reading the resource file: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
}
