/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.DiscoveryUtil;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.DataMonitor.Job.Status;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.Deserializer;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Stopwatch;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

@Command(name = "discover", description = "Discovers and defines data source or sink from data system configuration")
public class DiscoverCommand extends AbstractCommand {

  public static final long MAX_EXECUTION_TIME_DEFAULT_SEC = 3600;

  @Parameters(index = "0", description = "Data system configuration or data directory")
  private Path inputFile;

  @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory for data source/sink configuration (current directory by default)")
  private Path outputDir = null;

  @CommandLine.Option(names = {"-s", "--statistics"}, description = "Generates statistics for each table in the data source")
  private boolean statistics = false;

  @CommandLine.Option(names = {"-l", "--limit"}, description = "Maximum amount of time (in seconds) for running data analysis (1 hour by default).")
  private long maxExecutionTimeSec = MAX_EXECUTION_TIME_DEFAULT_SEC;

  @Override
  protected void runCommand(ErrorCollector errors) throws IOException {
    errors.checkFatal(!statistics, ErrorCode.NOT_YET_IMPLEMENTED, "Statistics generation not yet supported");
    DataSystemConfig discoveryConfig = null;
    if (inputFile != null && Files.isRegularFile(inputFile)) {
      Deserializer deserialize = new Deserializer();
      discoveryConfig = deserialize.mapJsonFile(inputFile, DataSystemConfig.class);
    } else if (inputFile != null && Files.isDirectory(inputFile)) {
      discoveryConfig = DiscoveryUtil.getDirectorySystemConfig(inputFile).build();
    } else {
      errors.fatal("Could not find data system configuration or directory at: %s", inputFile);
    }

    if (!discoveryConfig.getType().isSource() && statistics) {
      errors.warn("Data sinks don't have statistics. Statistics flag is ignored.");
      statistics = false;
    }
    errors.checkFatal(!statistics, ErrorCode.NOT_YET_IMPLEMENTED, "Statistics generation not yet supported");

    List<Path> packageFiles = PackagerUtil.getOrCreateDefaultPackageFiles(root);
    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFromPath(packageFiles,
        GlobalEngineConfiguration.class);
    EngineSettings engineSettings = engineConfig.initializeEngines(errors);
    DataDiscovery discovery = new DataDiscovery(errors, engineSettings);

    //Setup output directory to write to
    if (outputDir == null) {
      outputDir = root.rootDir;
    }
    Files.createDirectories(outputDir);

    List<TableInput> inputTables = discovery.discoverTables(discoveryConfig);
    errors.checkFatal(inputTables!=null && !inputTables.isEmpty(),"Did not discover any tables");
    DataMonitor.Job monitorJob = discovery.monitorTables(inputTables);
    errors.checkFatal(monitorJob!=null, "Could not build data discovery job");
    monitorJob.executeAsync("discovery");

    AtomicBoolean writeOnShutdown = new AtomicBoolean(true);
    Runnable writeOutput = () -> {
      try {
        List<TableSource> sourceTables = discovery.discoverSchema(inputTables);
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

    DataMonitor.Job.Status status = Status.RUNNING;
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
    if (status==Status.FAILED) {
      errors.fatal("Data discovery job failed");
    } else {
      writeOutput.run();
    }
  }
}
