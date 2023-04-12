package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import lombok.SneakyThrows;

/**
 * Used for stand alone flink jars
 */
public class FlinkMain {
  ErrorCollector errors = ErrorCollector.root();

  public static void main(String[] args) {
    (new FlinkMain()).run();
  }

  @SneakyThrows
  private void run() {

  }
}
