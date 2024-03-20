/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

import com.datasqrl.config.EngineKeys;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.DataDiscoveryFactory;
import com.datasqrl.discovery.MonitoringJobFactory;
import com.datasqrl.discovery.MonitoringJobFactory.Job;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.discovery.system.DataSystemDiscovery;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.server.GenericJavaServerEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.FileConfigOptions;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.packager.Packager;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import picocli.CommandLine;

@Command(name = "discover", description = "Discovers and defines data source or sink from data system configuration")
public class DiscoverCommand extends AbstractCommand {

  public static final long MAX_EXECUTION_TIME_DEFAULT_SEC = 3600;

  @Parameters(index = "0", description = "Data system configuration or data directory")
  private String systemConfig;

  @CommandLine.Option(names = {"-t", "--type"}, description = "The type of data system to connect to for discovery")
  private String systemType = null;

  @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory for data source/sink configuration (current directory by default)")
  private Path outputDir = null;

  @CommandLine.Option(names = {"-s", "--statistics"}, description = "Generates statistics for each table in the data source")
  private boolean statistics = false;




  @CommandLine.Option(names = {"-l", "--limit"}, description = "Maximum amount of time (in seconds) for running data analysis (1 hour by default).")
  private long maxExecutionTimeSec = MAX_EXECUTION_TIME_DEFAULT_SEC;

  @Override
  protected void execute(ErrorCollector errors) throws IOException {
    errors.checkFatal(!statistics, ErrorCode.NOT_YET_IMPLEMENTED, "Statistics generation not yet supported");
    String dataSystemType;
    if (Strings.isNullOrEmpty(systemType)) {
      //try to infer from the provided argument
      dataSystemType = DataDiscoveryFactory.inferDataSystemFromArgument(systemConfig).orElse(null);
    } else {
      dataSystemType = systemType;
    }
    errors.checkFatal(!Strings.isNullOrEmpty(dataSystemType),"Need to specify the type of the data system.");
    Optional<DataSystemDiscovery> systemDiscovery = DataSystemDiscovery.load(dataSystemType);
    errors.checkFatal(systemDiscovery.isPresent(), "Could not find data system [%s]. Available options are: %s", dataSystemType, DataSystemDiscovery.getAvailable());

    //check package json, or use embedded config
    SqrlConfig config = Packager.findPackageFile(root.rootDir, this.root.packageFiles)
        .map(p -> SqrlConfigCommons.fromFiles(errors, p))
        .orElseGet(() -> createEmbeddedConfig(errors));

    DataDiscovery discovery = DataDiscoveryFactory.fromConfig(config, errors);

    //Setup output directory to write to
    if (outputDir == null) {
      outputDir = root.rootDir;
    }
    Files.createDirectories(outputDir);

    //First, discover tables
    Collection<TableConfig> discoveredTables = systemDiscovery.get().discoverTables(discovery.getConfiguration(), systemConfig);

    errors.checkFatal(discoveredTables!=null && !discoveredTables.isEmpty(),"Did not discover any tables");
    MonitoringJobFactory.Job monitorJob = discovery.monitorTables(discoveredTables);
    errors.checkFatal(monitorJob!=null, "Could not build data discovery job");
    monitorJob.executeAsync("discovery");

    AtomicBoolean writeOnShutdown = new AtomicBoolean(true);
    Runnable writeOutput = () -> {
      try {
        List<TableSource> sourceTables = discovery.discoverSchema(discoveredTables);
        TableWriter writer = new TableWriter();
        writer.writeToFile(outputDir, sourceTables);
      } catch (IOException e) {
        errors.fatal("Could not write results to file: %s", e);
      }
    };

    //Register shutdown hook so results get written even if job is terminated
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (writeOnShutdown.get()) {
          writeOutput.run();
        }
      }
    });

    Job.Status status = Job.Status.RUNNING;
    Stopwatch timer = Stopwatch.createStarted();
    boolean canceled = false;
    while (!(status = monitorJob.getStatus()).hasStopped()) {
      if (timer.elapsed().toSeconds()>maxExecutionTimeSec && !canceled) {
        monitorJob.cancel();
        canceled = true;
      } else {
        //wait a little before checking status again
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
    writeOnShutdown.set(false);
    if (status==Job.Status.FAILED) {
      errors.fatal("Data discovery job failed");
    } else {
      writeOutput.run();
    }
  }

  private void applyConnectorOverrides(SqrlConfig connectorConfig) {
    connectorConfig.setProperty(FileConfigOptions.MONITOR_INTERVAL_MS, "0");
  }

  @SneakyThrows
  public SqrlConfig createEmbeddedConfig(ErrorCollector errors) {
    SqrlConfig rootConfig = SqrlConfig.createCurrentVersion(errors);

    SqrlConfig config = rootConfig.getSubConfig(PipelineFactory.ENGINES_PROPERTY);

    SqrlConfig dbConfig = config.getSubConfig("database");
    dbConfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
    dbConfig.setProperties(JdbcDataSystemConnector.builder()
        .url("jdbc:h2:file:./h2.db")
        .driver("org.h2.Driver")
        .dialect("h2")
        .database("datasqrl")
        .build()
    );

    SqrlConfig flinkConfig = config.getSubConfig(EngineKeys.STREAMS);
    flinkConfig.setProperty(FlinkEngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);

    SqrlConfig server = config.getSubConfig(EngineKeys.SERVER);
    server.setProperty(GenericJavaServerEngineFactory.ENGINE_NAME_KEY,
        VertxEngineFactory.ENGINE_NAME);

    return rootConfig;
  }



}
