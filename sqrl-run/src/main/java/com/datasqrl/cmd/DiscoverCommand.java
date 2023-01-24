/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.DiscoveryUtil;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.DataMonitor.Job.Status;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.Deserializer;
import com.datasqrl.service.PathUtil;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import picocli.CommandLine;

@Command(name = "discover", description = "Discovers the schema of a given data system or files in a directory")
public class DiscoverCommand extends AbstractCommand {

  @Parameters(index = "0", description = "Data system configuration or directory")
  private Path inputFile;

  @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory")
  private Path outputDir = null;

  @CommandLine.Option(names = {"-l", "--limit"}, description = "Limit the amount of time (in seconds) for running discovery")
  private long maxExecutionTimeSec = Long.MAX_VALUE;

  @Override
  protected void runCommand(ErrorCollector errors) throws IOException {
    DataSystemConfig discoveryConfig;
    if (inputFile != null && Files.isRegularFile(inputFile)) {
      Deserializer deserialize = new Deserializer();
      discoveryConfig = deserialize.mapJsonFile(inputFile, DataSystemConfig.class);
    } else if (inputFile != null && Files.isDirectory(inputFile)) {
      discoveryConfig = DiscoveryUtil.getDirectorySystemConfig(inputFile).build();
    } else {
      throw new IllegalArgumentException(
          "Could not find data system configuration or directory at: " + inputFile);
    }
    List<Path> packageFiles = PathUtil.getOrCreateDefaultPackageFiles(root);
    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFrom(packageFiles,
        GlobalEngineConfiguration.class);
    EngineSettings engineSettings = engineConfig.initializeEngines(errors);
    DataDiscovery discovery = new DataDiscovery(errors, engineSettings);

    //Setup output directory to write to
    if (outputDir == null) {
      outputDir = Path.of(discoveryConfig.getName());
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
