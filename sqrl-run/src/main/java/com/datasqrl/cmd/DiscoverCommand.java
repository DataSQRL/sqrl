package com.datasqrl.cmd;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.DiscoveryUtil;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.Deserializer;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

@Command(name = "discover", description = "Discovers the schema of a given data system or files in a directory")
public class DiscoverCommand extends AbstractCommand {

    @Parameters(index = "0", description = "Data system configuration or directory")
    private Path inputFile;

    @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory")
    private Path outputDir = null;

    @Override
    protected void runCommand(ErrorCollector errors) throws IOException {
        DataSystemConfig discoveryConfig;
        if (inputFile != null && Files.isRegularFile(inputFile)) {
            Deserializer deserialize = new Deserializer();
            discoveryConfig = deserialize.mapJsonFile(inputFile, DataSystemConfig.class);
        } else if (inputFile !=null && Files.isDirectory(inputFile)) {
            discoveryConfig = DiscoveryUtil.getDirectorySystemConfig(inputFile).build();
        } else {
            throw new IllegalArgumentException("Need to provide data system configuration or directory as input");
        }
        List<Path> packageFiles = getPackageFilesWithDefault(true);
        GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFrom(packageFiles,GlobalEngineConfiguration.class);
        EngineSettings engineSettings = engineConfig.initializeEngines(errors);
        DataDiscovery discovery = new DataDiscovery(errors, engineSettings);
        List<TableSource> sourceTables = discovery.runFullDiscovery(discoveryConfig);

        if (outputDir==null) {
            outputDir = Path.of(discoveryConfig.getName());
        }
        Files.createDirectories(outputDir);
        TableWriter writer = new TableWriter();
        writer.writeToFile(outputDir, sourceTables);
    }
}
