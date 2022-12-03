package com.datasqrl.schema;

import com.datasqrl.compile.loaders.DataSource;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.physical.stream.flink.schema.FlinkTableSchemaGenerator;
import com.datasqrl.physical.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.FlexibleTable2UTBConverter;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.external.DatasetDefinition;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.junit.ArgumentProvider;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the generation of schemas for various consumers based on the central {@link FlexibleDatasetSchema}
 * by way of the {@link UniversalTableBuilder}.
 */
public class FlexibleSchemaHandlingTest {

    @ParameterizedTest
    @ArgumentsSource(SchemaConverterProvider.class)
    public<S> void conversionTest(InputSchema inputSchema, SchemaConverterTestCase<S> visitorTest) {
        SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), inputSchema.getName(), visitorTest.schemaConverter.getClass().getSimpleName());
        Name tableAlias = Name.system("TestTable");
        FlexibleDatasetSchema schema = getSchema(inputSchema);
        for (FlexibleDatasetSchema.TableField table : schema.getFields()) {
            for (boolean hasSourceTimestamp : new boolean[]{true, false}) {
                for (Optional<Name> alias : new Optional[]{Optional.empty(), Optional.of(tableAlias)}) {
                    FlexibleTableConverter converter = new FlexibleTableConverter(new InputTableSchema(table, hasSourceTimestamp), alias);
                    FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter();
                    UniversalTableBuilder tblBuilder = converter.apply(utbConverter);
                    if (alias.isPresent()) {
                        assertEquals(tblBuilder.getName(),alias.get());
                        continue;
                    }
                    S resultSchema = visitorTest.schemaConverter.convertSchema(tblBuilder);
                    assertNotNull(resultSchema);
                    String[] caseName = getCaseName(table.getName().getDisplay(), hasSourceTimestamp);
                    snapshot.addContent(resultSchema.toString(), caseName);
                }
            }
        }
        snapshot.createOrValidate();
    }

    public static String[] getCaseName(String tableName, boolean hasTimestamp) {
        List<String> caseName = new ArrayList<>();
        caseName.add(tableName);
        if (hasTimestamp) caseName.add("hasTimestamp");
        return caseName.toArray(new String[caseName.size()]);
    }

    @SneakyThrows
    public FlexibleDatasetSchema getSchema(InputSchema inputSchema) {
        SchemaDefinition schemaDef = new DataSource.Loader().loadPackageSchema(inputSchema.packageDir);
        DatasetDefinition datasetDefinition = schemaDef.datasets.stream().filter(dd -> dd.name.equalsIgnoreCase(inputSchema.name)).findFirst().get();
        SchemaImport.DatasetConverter importer = new SchemaImport.DatasetConverter(NameCanonicalizer.SYSTEM, Constraint.FACTORY_LOOKUP);
        ErrorCollector errors = ErrorCollector.root();
        FlexibleDatasetSchema schema = importer.convert(datasetDefinition,errors);

        assertFalse(errors.isFatal(), errors.toString());
        assertFalse(schema.getFields().isEmpty());
        return schema;
    }

    @Value
    public static class InputSchema {
        Path packageDir;
        String name;
    }

    static class SchemaConverterProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<SchemaConverterTestCase> converters = new ArrayList<>();

            //Calcite
            CalciteTableFactory calciteTableFactory = new CalciteTableFactory(new JavaTypeFactoryImpl());
            CalciteTableFactory.UTB2RelDataTypeConverter converter = calciteTableFactory.new UTB2RelDataTypeConverter();
            converters.add(new SchemaConverterTestCase(converter));
            //Flink
            converters.add(new SchemaConverterTestCase(FlinkTypeInfoSchemaGenerator.INSTANCE));
            converters.add(new SchemaConverterTestCase(FlinkTableSchemaGenerator.INSTANCE));

            List<InputSchema> schemas = TestDataset.getAll().stream()
                    .map(td -> new InputSchema(td.getRootPackageDirectory().resolve(td.getName()),td.getName()))
                    .collect(Collectors.toList());

            return ArgumentProvider.crossProduct(schemas, converters);
        }
    }

    @Value
    @AllArgsConstructor
    static class SchemaConverterTestCase<S> {
        UniversalTableBuilder.SchemaConverter<S> schemaConverter;
        SchemaConverterValidator<S> validator;

        public SchemaConverterTestCase(UniversalTableBuilder.SchemaConverter<S> schemaConverter) {
            this(schemaConverter,null);
        }

    }

    @FunctionalInterface
    static interface SchemaConverterValidator<S> {

        void validate(S result, Name tableName);

    }


}
