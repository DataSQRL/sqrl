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

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.graphql.config.KafkaConfig;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.VertxJdbcClient;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.PaginationType;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryWithArguments;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(VertxExtension.class)
@Testcontainers
class WriteIT {

  @Container
  private final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.9.1"));

  @Container
  private final PostgreSQLContainer postgresContainer =
      new PostgreSQLContainer(
              DockerImageName.parse("ankane/pgvector:v0.5.0").asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  private SqlClient client;

  Vertx vertx;
  RootGraphqlModel model;
  String topicName = "topic-1";

  ServerConfig config;

  @SneakyThrows
  @BeforeEach
  void init(Vertx vertx) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(postgresContainer.getDatabaseName());
    options.setHost(postgresContainer.getHost());
    options.setPort(postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setUser(postgresContainer.getUsername());
    options.setPassword(postgresContainer.getPassword());

    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);

    config = mock(ServerConfig.class);
    when(config.getPgConnectOptions()).thenReturn(options);
    when(config.getKafkaMutationConfig())
        .thenReturn(new KafkaConfig.KafkaMutationConfig(getKafkaConfig()));

    var client =
        PgBuilder.client().with(new PoolOptions()).connectingTo(options).using(vertx).build();
    this.client = client;
    this.vertx = vertx;
    this.model = getCustomerModel();
  }

  private Map<String, String> getKafkaConfig() {
    var props = new HashMap<String, String>();
    props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(GROUP_ID_CONFIG, "kafka-test-listener");
    props.put(
        KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    return props;
  }

  private RootGraphqlModel getCustomerModel() {
    return RootGraphqlModel.builder()
        .schema(
            StringSchema.builder()
                .schema(
                    """
                scalar DateTime
                type Query { \
                  customer: Customer \
                } \
                type Mutation {\
                  addCustomer(event: CreateCustomerEvent): Customer\
                } \
                input CreateCustomerEvent {\
                  customerid: Int\
                  ts: DateTime\
                } \
                type Customer {\
                  customerid: Int \
                  ts: DateTime\
                }\
                """)
                .build())
        .query(
            ArgumentLookupQueryCoords.builder()
                .parentType("Query")
                .fieldName("customer")
                .exec(
                    QueryWithArguments.builder()
                        .query(
                            new SqlQuery(
                                "SELECT customerid FROM Customer",
                                List.of(),
                                PaginationType.NONE,
                                0,
                                DatabaseType.POSTGRES))
                        .build())
                .build())
        .mutation(
            new KafkaMutationCoords("addCustomer", false, topicName, Map.of(), false, Map.of()))
        .build();
  }

  @AfterEach
  void after() {
    client.close();
  }

  @SneakyThrows
  @Test
  void test() {
    // Create topic using AdminClient
    var adminProps = new Properties();
    adminProps.put(BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    try (var adminClient = AdminClient.create(adminProps)) {
      var newTopic = new NewTopic(topicName, 1, (short) 1);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    }

    var props = new Properties();
    props.putAll(getKafkaConfig());
    var consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Collections.singletonList(topicName));

    GraphQL graphQL =
        model
            .accept(
                new GraphQLEngineBuilder.Builder()
                    .withMutationConfiguration(new MutationConfigurationImpl(vertx, config))
                    .withSubscriptionConfiguration(new SubscriptionConfigurationImpl(vertx, config))
                    .build(),
                new VertxContext(
                    new VertxJdbcClient(Map.of(DatabaseType.POSTGRES, client)), null, null))
            .build();

    ExecutionInput executionInput =
        ExecutionInput.newExecutionInput()
            .query(
                "mutation ($event: CreateCustomerEvent!) { addCustomer(event: $event) { customerid,"
                    + " ts } }")
            .variables(
                Map.of("event", Map.of("customerid", 123, "ts", "2001-01-01T10:00:00-05:00")))
            .build();

    ExecutionResult executionResult = graphQL.execute(executionInput);

    Map<String, Object> data = executionResult.getData();
    assertThat(data).hasSize(1);
    assertThat(data.get("addCustomer"))
        .hasToString("{customerid=123, ts=2001-01-01T10:00:00.000-05:00}");

    try {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      assertThat(records.isEmpty()).isFalse();

      for (ConsumerRecord<String, String> record : records) {
        assertThat(record.value()).startsWith("{\"customerid\":123");
      }
    } catch (WakeupException e) {
      // Ignore exception
    } finally {
      consumer.close();
    }
  }
}
