package com.datasqrl.engine.log;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.KafkaMutationCoords;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.serializer.Deserializer;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
@AutoService(EngineFactory.class)
public class KafkaLogEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "kafka";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.LOG;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new KafkaLogEngine(config);
  }

  public static class KafkaLogEngine extends ExecutionEngine.Base implements ExecutionEngine {

    private final SqrlConfig config;
    private List<String> topics;

    public KafkaLogEngine(@NonNull SqrlConfig config) {
      super(ENGINE_NAME, Type.LOG, EngineCapability.NO_CAPABILITIES);
      this.config = config;
    }

    @Override
    public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan,
        ErrorCollector errors) {
      //Start up in-mem kafka instance, run init script
      Properties props = new Properties();
      props.put("bootstrap.servers", config.asString("bootstrap.servers").get());
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-topic-event-lister");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

      try (var admin = AdminClient.create(props)) {
        for (String topic : topics) {
          admin.createTopics(List.of(new NewTopic(topic, Optional.empty(), Optional.empty())));
        }
      }
      return null;
    }

    @Override
    public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
        ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink) {
      this.topics = plan.getModel().getMutations().stream()
          .map(e->(KafkaMutationCoords) e)
          .map(KafkaMutationCoords::getTopic)
          .collect(Collectors.toList());
      return new LogEnginePhysicalPlan(this.topics);
    }

    @Override
    public void generateAssets(Path buildDir) {
    }
  }

  @AllArgsConstructor
  public static class LogEnginePhysicalPlan implements EnginePhysicalPlan {

    private final List<String> topics;

    @Override
    public void writeTo(Path deployDir, String stageName, Deserializer serializer)
        throws IOException {

    }
  }
}
