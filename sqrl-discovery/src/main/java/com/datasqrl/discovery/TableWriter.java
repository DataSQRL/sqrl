package com.datasqrl.discovery;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.name.Name;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaExport;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.NonNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class TableWriter {

    private final ObjectMapper jsonMapper;
    private final YAMLMapper yamlMapper;

    public TableWriter() {
        this.jsonMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);;
        this.yamlMapper = new YAMLMapper();
        this.yamlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void writeToFile(@NonNull Path destinationDir, @NonNull List<TableSource> tables) throws IOException {
        //Write out table configurations
        for (TableSource table : tables) {
            Path tableConfigFile = destinationDir.resolve(table.getName().getCanonical() + DataSource.TABLE_FILE_SUFFIX);
            jsonMapper.writeValue(tableConfigFile.toFile(), table.getConfiguration());
        }

        Name datasetName = tables.get(0).getPath().parent().getLast();
        FlexibleDatasetSchema combinedSchema = DataDiscovery.combineSchema(tables);

        //Write out combined schema file
        SchemaExport export = new SchemaExport();
        SchemaDefinition outputSchema = export.export(Map.of(datasetName, combinedSchema));
        Path schemaFile = destinationDir.resolve(DataSource.PACKAGE_SCHEMA_FILE);
        yamlMapper.writeValue(schemaFile.toFile(),outputSchema);
    }

}
