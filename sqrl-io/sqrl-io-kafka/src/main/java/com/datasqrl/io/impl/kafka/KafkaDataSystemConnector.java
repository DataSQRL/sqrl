package com.datasqrl.io.impl.kafka;

import com.datasqrl.io.DataSystemConnector;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class KafkaDataSystemConnector implements DataSystemConnector {

  @Override
  public boolean hasSourceTimestamp() {
    return true;
  }


}
