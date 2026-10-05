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
package com.datasqrl.util;

import static com.datasqrl.env.EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_JDBC_URL;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;

import com.datasqrl.deployment.model.JdbcPlanModel;
import com.datasqrl.deployment.model.KafkaNewTopicModel;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CmdUtils {

  private static final int TOPIC_CREATE_TIMEOUT_MS = 8000;

  @SneakyThrows
  public static void initializePostgres(Path planDir, Map<String, String> env) {
    var postgresPlan = getPostgresPlan(planDir);
    if (postgresPlan.isEmpty()) {
      log.debug("The Postgres physical plan is empty, skip init");
      return;
    }

    try (var connection = postgresConnection(env)) {
      for (var jdbcStmt : postgresPlan.get().statements()) {
        log.info("Executing statement {} of type {}", jdbcStmt.name(), jdbcStmt.type());
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(jdbcStmt.sql());
        } catch (Exception e) {
          e.printStackTrace();
          assert false : e.getMessage();
        }
      }
      // Extension statements manage the lifecycle of extension-backed tables (e.g. pg_partman
      // creates the partitions of ttl() tables); without them partitioned parents have no
      // partitions and every insert fails. Failures are non-fatal so plans still run against
      // a Postgres that lacks the extension.
      for (var jdbcStmt : postgresPlan.get().standaloneExtensionStatements()) {
        log.info(
            "Executing standalone extension statement {} of type {}",
            jdbcStmt.name(),
            jdbcStmt.type());
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(jdbcStmt.sql());
        } catch (Exception e) {
          log.warn(
              "Failed to execute standalone extension statement '{}'. The required Postgres"
                  + " extension may not be installed; extension-managed features will be"
                  + " unavailable.",
              jdbcStmt.name(),
              e);
        }
      }
    }
  }

  @SneakyThrows
  public static void resetPostgres(Path planDir, Map<String, String> env) {
    if (getPostgresPlan(planDir).isEmpty()) {
      return;
    }

    try (var connection = postgresConnection(env);
        var statement = connection.createStatement()) {
      statement.execute(
          """
          DO $$
          DECLARE table_record RECORD;
          BEGIN
            FOR table_record IN
              (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
            LOOP
              EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_record.tablename) || ' CASCADE';
            END LOOP;
          END $$;
          """);
    }
  }

  @SneakyThrows
  public static void initializeKafka(Path planDir, Map<String, String> env) {
    var topicsToCreate = getKafkaTopics(planDir);
    if (topicsToCreate.isEmpty()) {
      log.debug("The Kafka physical plan is empty, skip init");
      return;
    }

    try (var adminClient = kafkaAdminClient(env)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();
      for (var topicName : topicsToCreate) {
        if (!existingTopics.contains(topicName)) {
          // We need to limit both partitions and replication factor to 1 here,
          // cause this will run on the Redpanda "cluster" inside the cmd image.
          adminClient
              .createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)))
              .all()
              .get();
        }
      }
    } catch (Exception e) {
      log.warn(
          "Failed to create Kafka topic(s). One or more required topics for the SQRL pipeline might not exist."
              + " Please ensure all topics are pre-created, as automatic topic creation is only available for the internal Kafka instance.");
      log.debug("Topic creation error details:", e);
    }
  }

  @SneakyThrows
  public static void resetKafka(Path planDir, Map<String, String> env) {
    var topics = getKafkaTopics(planDir);
    if (topics.isEmpty()) {
      return;
    }

    try (var adminClient = kafkaAdminClient(env)) {
      topics.retainAll(adminClient.listTopics().names().get());
      if (!topics.isEmpty()) {
        adminClient.deleteTopics(topics).all().get();
      }
    }
  }

  private static Optional<JdbcPlanModel> getPostgresPlan(Path planDir) {
    return ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir)
        .filter(plan -> !plan.statements().isEmpty());
  }

  private static Set<String> getKafkaTopics(Path planDir) {
    return ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir)
        .filter(plan -> !plan.isEmpty())
        .map(
            plan ->
                Stream.concat(plan.topics().stream(), plan.testRunnerTopics().stream())
                    .map(KafkaNewTopicModel::topicName)
                    .collect(Collectors.toCollection(HashSet::new)))
        .orElseGet(HashSet::new);
  }

  private static Connection postgresConnection(Map<String, String> env) throws SQLException {
    return DriverManager.getConnection(
        getRequiredEnv(env, POSTGRES_JDBC_URL),
        getRequiredEnv(env, POSTGRES_USERNAME),
        getRequiredEnv(env, POSTGRES_PASSWORD));
  }

  private static AdminClient kafkaAdminClient(Map<String, String> env) {
    var bootstrapServers = env.get(KAFKA_BOOTSTRAP_SERVERS);
    if (bootstrapServers == null) {
      throw new IllegalStateException(
          "Failed to get Kafka 'bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS is not set");
    }

    var properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TOPIC_CREATE_TIMEOUT_MS);
    return AdminClient.create(properties);
  }

  private static String getRequiredEnv(Map<String, String> env, String envVarName) {
    return Objects.requireNonNull(
        env.get(envVarName), "Missing environment variable: " + envVarName);
  }
}
