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

<#list flink["flinkSql"] as sql>
    tableResult = tEnv.executeSql(replaceWithEnv(""
        <#list sql?split("\n") as line>
        + "${line?replace("\"", "\\\"")?replace("\\", "\\\\")} "
        </#list>
        ));
</#list>
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
}
