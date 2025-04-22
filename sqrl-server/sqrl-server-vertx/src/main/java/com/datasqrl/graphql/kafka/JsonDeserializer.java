package com.datasqrl.graphql.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.datasqrl.graphql.SqrlObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class JsonDeserializer implements Deserializer<Map> {
  private String encoding;

  public JsonDeserializer() {
    this.encoding = StandardCharsets.UTF_8.name();
  }

  @Override
public void configure(Map<String, ?> configs, boolean isKey) {
    var propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) {
      encodingValue = configs.get("deserializer.encoding");
    }

    if (encodingValue instanceof String string) {
      this.encoding = string;
    }

  }

  @Override
public Map deserialize(String topic, byte[] data) {
    try {
      return data == null ? null : SqrlObjectMapper.mapper.readValue(new String(data, this.encoding), Map.class);
    } catch (UnsupportedEncodingException var4) {
      throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
