/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import java.nio.file.Files;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractCommand implements Runnable {

  @CommandLine.ParentCommand
  protected RootCommand root;
  EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
  protected boolean startKafka;

  @SneakyThrows
  public void run() {
    ErrorCollector collector = ErrorCollector.root();
    try {
      runCommand(collector);
      root.statusHook.onSuccess();
    } catch (Exception e) {
      collector.getCatcher().handle(e);
      e.printStackTrace();
      root.statusHook.onFailure();
    } finally {
      if (startKafka) {
        CLUSTER.stop();
      }
    }
    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void runCommand(ErrorCollector errors) throws Exception;

}
