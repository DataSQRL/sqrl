/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;

import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.EngineKeys;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfig.Value;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.PackagerUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true, true, true);
  }

  @Override
  protected SqrlConfig initializeConfig(DefaultConfigSupplier configSupplier, ErrorCollector errors) {
    SqrlConfig defaultConfig = getDefaultConfig(true, errors).get();
    SqrlConfig userConfig = PackagerUtil.getOrCreateDefaultConfiguration(root,
        errors, () -> defaultConfig);

    // Overwrite the bootstrap servers since they change per invocation
    Value<String> bootstrapServers = userConfig
        .getSubConfig(EngineKeys.ENGINES)
        .getSubConfig(EngineKeys.LOG)
        .getSubConfig(CONNECTOR_KEY)
        .asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

    if (bootstrapServers.getOptional().isEmpty()) {
      userConfig
          .getSubConfig(EngineKeys.ENGINES)
          .getSubConfig(EngineKeys.LOG)
          .getSubConfig(CONNECTOR_KEY)
          .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    }

    return userConfig;
  }

  @Override
  protected void postCompileActions(DefaultConfigSupplier configSupplier, Packager packager,
      CompilerResult result, ErrorCollector errors) {
    super.postCompileActions(configSupplier, packager, result, errors);

    executePlan(result.getPlan(), errors);
  }
}
