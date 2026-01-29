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
package com.datasqrl.graphql.kafka;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.graphql.config.ServerConfigProperties;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.MutationConfiguration;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Spring-based mutation configuration that uses Spring Kafka to send messages. Replaces the
 * Vert.x-based MutationConfigurationImpl.
 */
@Slf4j
@RequiredArgsConstructor
public class SpringMutationConfiguration implements MutationConfiguration<DataFetcher<?>> {

  private final ServerConfigProperties config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public MutationCoordsVisitor<DataFetcher<?>, Context> createSinkFetcherVisitor() {
    return (coords, context) -> {
      var kafkaConfig = config.getKafkaMutation();
      if (kafkaConfig == null) {
        throw new RuntimeException("Kafka mutation configuration is not set");
      }

      var kafkaTemplate = createKafkaTemplate(kafkaConfig, coords.isTransactional());
      var topic = coords.getTopic();

      var computedInputColumns = new HashMap<String, ComputeInputColumns>();
      coords
          .getComputedColumns()
          .forEach(
              (colName, metadata) -> {
                ComputeInputColumns fct =
                    switch (metadata.metadataType()) {
                      case UUID -> (env -> UUID.randomUUID());
                      case AUTH -> {
                        MetadataReader metadataReader =
                            context.getMetadataReader(metadata.metadataType());
                        yield (env ->
                            metadataReader.read(env, metadata.name(), metadata.required()));
                      }
                      default -> null;
                    };
                if (fct != null) {
                  computedInputColumns.put(colName, fct);
                }
              });

      var timestampColumns =
          coords.getComputedColumns().entrySet().stream()
              .filter(e -> e.getValue().metadataType() == MetadataType.TIMESTAMP)
              .map(Entry::getKey)
              .toList();

      checkNotNull(topic, "Could not find topic for field: %s", coords.getFieldName());

      return env -> {
        var entries = getEntries(env, computedInputColumns);
        var cf = new CompletableFuture<Object>();

        if (coords.isTransactional()) {
          sendMessagesTransactionally(
              kafkaTemplate, topic, entries, timestampColumns, cf, coords.isReturnList());
        } else {
          sendMessagesNonTransactionally(
              kafkaTemplate, topic, entries, timestampColumns, cf, coords.isReturnList());
        }

        return cf;
      };
    };
  }

  @FunctionalInterface
  interface ComputeInputColumns {
    Object compute(DataFetchingEnvironment env);
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getEntries(
      DataFetchingEnvironment env, Map<String, ComputeInputColumns> computedColumns) {

    var args = env.getArguments();
    var argument = args.entrySet().stream().findFirst().map(Entry::getValue).orElse(null);

    List<Map<String, Object>> entries;
    if (argument instanceof List) {
      entries = (List<Map<String, Object>>) argument;
    } else {
      entries = List.of((Map<String, Object>) argument);
    }

    if (!computedColumns.isEmpty()) {
      entries.forEach(
          entry -> computedColumns.forEach((colName, fct) -> entry.put(colName, fct.compute(env))));
    }

    return entries;
  }

  private KafkaTemplate<String, String> createKafkaTemplate(
      ServerConfigProperties.KafkaMutationConfig kafkaConfig, boolean transactional) {

    Map<String, Object> props = new HashMap<>();
    kafkaConfig.getProperties().forEach(props::put);
    if (transactional) {
      props.put("transactional.id", UUID.randomUUID().toString());
    }

    var producerFactory =
        new org.springframework.kafka.core.DefaultKafkaProducerFactory<String, String>(props);
    return new KafkaTemplate<>(producerFactory);
  }

  private void sendMessagesNonTransactionally(
      KafkaTemplate<String, String> kafkaTemplate,
      String topic,
      List<Map<String, Object>> entries,
      List<String> timestampColumns,
      CompletableFuture<Object> cf,
      boolean returnList) {

    var futures =
        entries.stream()
            .map(entry -> sendMessage(kafkaTemplate, topic, entry, timestampColumns))
            .toList();

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .whenComplete(
            (v, throwable) -> {
              if (throwable != null) {
                cf.completeExceptionally(throwable);
              } else {
                var results =
                    futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
                completeWithResults(cf, results, returnList);
              }
            });
  }

  private void sendMessagesTransactionally(
      KafkaTemplate<String, String> kafkaTemplate,
      String topic,
      List<Map<String, Object>> entries,
      List<String> timestampColumns,
      CompletableFuture<Object> cf,
      boolean returnList) {

    try {
      kafkaTemplate.executeInTransaction(
          operations -> {
            List<Map<String, Object>> results =
                entries.stream()
                    .map(entry -> sendMessageSync(operations, topic, entry, timestampColumns))
                    .collect(Collectors.toList());
            completeWithResults(cf, results, returnList);
            return null;
          });
    } catch (Exception e) {
      cf.completeExceptionally(e);
    }
  }

  private CompletableFuture<Map<String, Object>> sendMessage(
      KafkaTemplate<String, String> kafkaTemplate,
      String topic,
      Map<String, Object> entry,
      List<String> timestampColumns) {

    try {
      var json = objectMapper.writeValueAsString(entry);
      var record = new ProducerRecord<String, String>(topic, null, json);

      var cf = new CompletableFuture<Map<String, Object>>();
      kafkaTemplate
          .send(record)
          .whenComplete(
              (result, throwable) -> {
                if (throwable != null) {
                  cf.completeExceptionally(throwable);
                } else {
                  var timestamp = result.getRecordMetadata().timestamp();
                  var dateTime =
                      ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
                  timestampColumns.forEach(
                      colName -> entry.put(colName, dateTime.toOffsetDateTime()));
                  cf.complete(entry);
                }
              });
      return cf;
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private Map<String, Object> sendMessageSync(
      org.springframework.kafka.core.KafkaOperations<String, String> operations,
      String topic,
      Map<String, Object> entry,
      List<String> timestampColumns) {

    try {
      var json = objectMapper.writeValueAsString(entry);
      var record = new ProducerRecord<String, String>(topic, null, json);
      var result = operations.send(record).get();
      var timestamp = result.getRecordMetadata().timestamp();
      var dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
      timestampColumns.forEach(colName -> entry.put(colName, dateTime.toOffsetDateTime()));
      return entry;
    } catch (Exception e) {
      throw new RuntimeException("Failed to send Kafka message", e);
    }
  }

  private void completeWithResults(
      CompletableFuture<Object> cf, List<Map<String, Object>> results, boolean returnList) {

    if (returnList) {
      cf.complete(results);
    } else {
      cf.complete(results.get(0));
    }
  }
}
