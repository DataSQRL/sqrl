package com.datasqrl.graphql.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.datasqrl.graphql.SqrlObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class JsonSerializer implements Serializer<Map> {
  private String encoding;

  public JsonSerializer() {
    this.encoding = StandardCharsets.UTF_8.name();
  }

  @Override
public void configure(Map<String, ?> configs, boolean isKey) {
    var propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) {
      encodingValue = configs.get("serializer.encoding");
    }

    if (encodingValue instanceof String string) {
      this.encoding = string;
    }

  }

  @Override
public byte[] serialize(String topic, Map data) {
    try {
      return data == null ? null :
          SqrlObjectMapper.mapper.writeValueAsString(data).getBytes(this.encoding);
    } catch (UnsupportedEncodingException var4) {
      throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + this.encoding);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}