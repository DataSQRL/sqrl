package com.datasqrl.io;

import com.datasqrl.io.util.TimeAnnotatedRecord;
import java.io.IOException;
import java.time.Instant;
import lombok.Getter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Getter
public class KafkaTimeValueDeserializationSchemaWrapper<T> implements
    KafkaRecordDeserializationSchema<TimeAnnotatedRecord<T>> {

  private static final long serialVersionUID = 1L;
  private final DeserializationSchema<T> deserializationSchema;

  KafkaTimeValueDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    deserializationSchema.open(context);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> message,
      Collector<TimeAnnotatedRecord<T>> out)
      throws IOException {
    T result = deserializationSchema.deserialize(message.value());
    if (result != null) {
      out.collect(new TimeAnnotatedRecord<>(result, Instant.ofEpochSecond(message.timestamp())));
    }
  }

  @Override
  public TypeInformation<TimeAnnotatedRecord<T>> getProducedType() {
    return (TypeInformation) TypeInformation.of(TimeAnnotatedRecord.class);
    //return deserializationSchema.getProducedType();
  }
}
