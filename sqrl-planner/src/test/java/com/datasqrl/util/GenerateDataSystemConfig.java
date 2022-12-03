package com.datasqrl.util;

import com.datasqrl.compile.loaders.DataSource;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

public class GenerateDataSystemConfig {

    @Test
    @Disabled
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

        Path datasystemConfigFile = testDataset.getRootPackageDirectory().resolve("output").resolve(DataSource.DATASYSTEM_FILE);
        FileTestUtil.writeJson(datasystemConfigFile, config);
    }

}
