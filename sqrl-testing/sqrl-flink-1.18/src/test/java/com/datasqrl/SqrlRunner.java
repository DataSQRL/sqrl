package com.datasqrl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
public class SqrlRunner {

  @Test
  public void test() {
    Path target = Path.of(
        "/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/snowflake/build/deploy");

    run(target);
  }

  @SneakyThrows
  public void run(Path target) {
    //Reads a deployment path and creates light-weight services from them
    //1. Read package-json to get enabled engines
    //2. create embedded engines

    //Get config map from flink
    Map conf = YAMLMapper.builder().build()
        .readValue(target.resolve("flink/src/main/resources/flink-conf.yaml").toFile(), Map.class);

    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
        Configuration.fromMap(conf));
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(conf)).build();

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
//    EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
//    CLUSTER.start();

    TableResult tableResult = null;
    String[] statements = Files.readString(target.resolve("flink/src/main/resources/flink.sql")).split("\n\n");
    for (String statement : statements) {
      if (statement.trim().isEmpty()) continue;
      tableResult = tEnv.executeSql(replaceWithEnv(statement, System.getenv()));
    }

    tableResult.print();
  }

  public String replaceWithEnv(String command, Map<String, String> env) {
//    Map<String, String> envVariables = System.getenv();
    Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");

    String substitutedStr = command;
    StringBuffer result = new StringBuffer();
    // First pass to replace environment variables
    Matcher matcher = pattern.matcher(substitutedStr);
    while (matcher.find()) {
      String key = matcher.group(1);
      String envValue = env.getOrDefault(key, "");
      matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
    }
    matcher.appendTail(result);

    System.out.println(result.toString());

    return result.toString();
  }
}
