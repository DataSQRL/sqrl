/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentSet;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.google.common.collect.Maps;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(VertxExtension.class)
@Testcontainers
class WriteTest {

  // will be started before and stopped after each test method
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
          .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

  //Todo Add Kafka

  private PgPool client;
  Map<String, SinkProducer> mutations;
  Map<String, SinkConsumer> subscriptions;

  RootGraphqlModel model;
  String topicName = "topic-1";

  @SneakyThrows
  @BeforeEach
  public void init(Vertx vertx) {
    CLUSTER.start();


    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(testDatabase.getDatabaseName());
    options.setHost(testDatabase.getHost());
    options.setPort(testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setUser(testDatabase.getUsername());
    options.setPassword(testDatabase.getPassword());

    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);

    PgPool client = PgPool.pool(vertx, options, new PoolOptions());
    this.client = client;
    this.model = getCustomerModel();
    GraphQLServer graphQLServer = new GraphQLServer(null, null, null);
    GraphQLServer serverSpy = Mockito.spy(graphQLServer);
    Mockito.when(serverSpy.getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"))
        .thenReturn(CLUSTER.bootstrapServers());

    this.mutations = serverSpy.constructSinkProducers(model, vertx);
    this.subscriptions = serverSpy.constructSubscriptions(model, vertx, Promise.promise());
  }

  private Properties getKafkaProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", CLUSTER.bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-listener");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    return props;
  }

  private RootGraphqlModel getCustomerModel() {
    return RootGraphqlModel.builder()
            .schema(StringSchema.builder().schema(""
                    + "scalar DateTime\n"
                + "type Query { "
                    + "  customer: Customer "
                    + "} "
                    + "type Mutation {"
                    + "  addCustomer(event: CreateCustomerEvent): Customer"
                    + "} "
                    + "input CreateCustomerEvent {"
                    + "  customerid: Int"
                    + "  ts: DateTime"
                    + "} "
                    + "type Customer {"
                    + "  customerid: Int "
                    + "  ts: DateTime"
                    + "}").build())
            .coord(ArgumentLookupCoords.builder()
                    .parentType("Query")
                    .fieldName("customer")
                    .match(ArgumentSet.builder()
                            .query(new JdbcQuery("postgres","SELECT customerid FROM Customer", List.of()))
                            .build())
                    .build())
            .mutation(new MutationCoords("addCustomer", topicName,
                Map.of()))
            .build();
  }

  @AfterEach
  public void after() {
    client.close();
  }

  @SneakyThrows
  @Test
  public void test() {

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProps());

    CLUSTER.createTopic(topicName);

    consumer.subscribe(Collections.singletonList(topicName));

    GraphQL graphQL = model.accept(
        new GraphQLEngineBuilder(),
        new VertxContext(new VertxJdbcClient(Map.of("postgres",client)), mutations, subscriptions, NameCanonicalizer.SYSTEM));

    ExecutionInput executionInput = ExecutionInput.newExecutionInput()
        .query("mutation ($event: CreateCustomerEvent!) { addCustomer(event: $event) { customerid, ts } }")
        .variables(Map.of("event", Map.of("customerid", 123, "ts", "2001-01-01T10:00:00-05:00")))
        .build();

    ExecutionResult executionResult = graphQL.execute(executionInput);

    Map<String, Object> data = executionResult.getData();
    assertEquals(1, data.size());
    assertEquals("{customerid=123, ts=2001-01-01T10:00:00.000-05:00}", data.get("addCustomer").toString());

    try {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      assertFalse(records.isEmpty());

      for (ConsumerRecord<String, String> record : records) {
        assertTrue(record.value().startsWith("{\"customerid\":123"));
      }
    } catch (WakeupException e) {
      // Ignore exception
    } finally {
      consumer.close();
    }
  }
}