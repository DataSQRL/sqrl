package com.datasqrl.cmd;

import com.datasqrl.util.Configuration;
import com.datasqrl.util.ConfigurationMapper;
import com.datasqrl.util.RandomSampler;
import com.github.javafaker.Faker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractGenerateCommand implements Runnable {

  @CommandLine.ParentCommand
  protected RootGenerateCommand root;

  @SneakyThrows
  protected<C extends Configuration> C getConfiguration(C configObject) {
    configObject.scale(getScaleFactor(), root.number);
    ConfigurationMapper.readProperties(configObject, root.configFile);
    return configObject;
  }

  protected long getScaleFactor() {
    return Math.max(1,root.number/root.BASE_NUMBER);
  }

  @SneakyThrows
  protected Path getOutputDir() {
    if (!Files.isDirectory(root.outputDirectory)) {
      Files.createDirectories(root.outputDirectory);
    }
    return root.outputDirectory;
  }


  protected transient Random random = null;
  protected transient Faker faker = null;
  protected transient RandomSampler sampler = null;

  protected synchronized void initialize() {
    if (random==null) {
      long seed = root.getSeed();
      if (seed != 0) random = new Random(root.getSeed());
      else random = new Random();
      faker = new Faker(root.getLocale(), random);
      sampler = new RandomSampler(random);
    }
  }

  public static Instant getStartTime(long numDaysInPast) {
    return ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).truncatedTo(
        ChronoUnit.DAYS).minus(numDaysInPast,ChronoUnit.DAYS).toInstant();
  }

}
