package ai.datasqrl.util;

import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.io.formats.FileFormat;
import ai.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.DataSystemDiscoveryConfig;
import ai.datasqrl.io.sources.ExternalDataType;
import ai.datasqrl.util.data.Retail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

public class GenerateDataSystemConfig {

    @Test
    @SneakyThrows
    public void generateConfigFile() {
        TestDataset testDataset = Retail.INSTANCE;
        Path output = testDataset.getRootPackageDirectory().resolve("export-data");
        DataSystemDiscoveryConfig datasystem = DirectoryDataSystemConfig.of(output);
        DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
        builder.datadiscovery(datasystem);
        builder.type(ExternalDataType.SINK);
        builder.name("output");
        builder.format(FileFormat.JSON.getImplementation().getDefaultConfiguration());
        DataSystemConfig config = builder.build();

        ObjectMapper jsonMapper = new ObjectMapper();
        jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
        Path datasystemConfigFile = testDataset.getRootPackageDirectory().resolve("output").resolve(DataSourceLoader.DATASYSTEM_FILE);
        jsonMapper.writeValue(datasystemConfigFile.toFile(),config);
    }

}
