package com.datasqrl.cmd;

import com.datasqrl.data.GenerateClickstream;
import com.datasqrl.data.GenerateLoans;
import com.datasqrl.data.GenerateSensors;
import java.nio.file.Path;
import java.util.Locale;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@CommandLine.Command(name = "datasqrl", mixinStandardHelpOptions = true, version = "0.1",
    subcommands = {GenerateClickstream.class, GenerateSensors.class, GenerateLoans.class})
@Getter
public class RootGenerateCommand implements Runnable {

  @CommandLine.Option(names = {"-c", "--config"}, description = "Properties configuration file"
      , scope = ScopeType.INHERIT)
  protected Path configFile = Path.of("config.properties");

  public static final long BASE_NUMBER = 1000;

  @CommandLine.Option(names = {"-n", "--number"}, description = "Number of records to generate"
      , scope = ScopeType.INHERIT)
  protected long number = BASE_NUMBER;

  public static final long BASE_SEED = 5292935023423l;

  @CommandLine.Option(names = {"--seed"}, description = "Seed for random generator. Set to 0 to generate seed"
      , scope = ScopeType.INHERIT)
  protected long seed = BASE_NUMBER;

  public static final Locale BASE_LOCALE = Locale.US;

  @CommandLine.Option(names = {"--locale"}, description = "Locale to use for data generation"
      , scope = ScopeType.INHERIT)
  protected Locale locale = BASE_LOCALE;

  @CommandLine.Option(names = {"-o","--output"}, description = "Directory that generated data is written to"
      , scope = ScopeType.INHERIT)
  protected Path outputDirectory = Path.of("data/");

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public CommandLine getCmd() {
    return new CommandLine(this).setCaseInsensitiveEnumValuesAllowed(true);
  }
}

