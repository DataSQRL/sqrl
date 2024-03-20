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
    List<String> sql = getPlan();
    Map<String, String> flinkConfig = getConfig();

    Configuration sEnvConfig = Configuration.fromMap(flinkConfig);
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
        sEnvConfig);
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(flinkConfig)).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;
    for (String command : sql) {
      String trim = command.trim();
      if (!trim.isEmpty()) {
        tableResult = tEnv.executeSql(trim);
      }
    }
    return tableResult;
  }

  private List<String> getPlan() throws Exception {
    URL resource = getClass().getResource("/flink-plan.sql");
    if (resource == null) {
      throw new RuntimeException("Could not find flink-plan.sql in jar");
    }
    String s = readFile(resource);
    return List.of(s.split("\n\n"));
  }

  private Map<String, String> getConfig() throws IOException {
    URL resource1 = getClass().getResource("/flink-conf.yaml");
    if (resource1 == null) {
      return Map.of();
    }
    Properties properties = new Properties();
    properties.load(resource1.openStream());
    Map<String, String> conf = new HashMap<>();
    Iterator<Object> keys = properties.keys().asIterator();
    while (keys.hasNext()) {
      String key = (String)keys.next();
      conf.put(key, properties.getProperty(key));
    }
    return conf;
  }

  public static String readFile(URL url) throws Exception {
    StringBuilder content = new StringBuilder();

    try (InputStream inputStream = url.openStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        content.append(line).append(System.lineSeparator());
      }
    }

    return content.toString();
  }
}
