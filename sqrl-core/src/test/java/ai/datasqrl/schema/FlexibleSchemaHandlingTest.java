package ai.datasqrl.schema;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.stream.flink.schema.FlinkTableSchemaGenerator;
import ai.datasqrl.physical.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.table.CalciteTableFactory;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.InputTableSchema;
import ai.datasqrl.util.TestDataset;
import lombok.Value;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the generation of schemas for various consumers based on the central {@link FlexibleDatasetSchema}
 * using the {@link FlexibleTableConverter}.
 */
@Disabled
public class FlexibleSchemaHandlingTest extends AbstractSchemaIT {

    @BeforeEach
    public void setup() {
        initialize(IntegrationTestSettings.getInMemory(false));
    }

    @ParameterizedTest
    @ArgumentsSource(SchemaConverterProvider.class)
    public<T, V extends FlexibleTableConverter.Visitor<T>> void conversionTest(TestDataset example, SchemaConverter<T,V> visitorTest) {
        Name tableAlias = Name.system("TestTable");
        registerDataset(example);
        FlexibleDatasetSchema schema = getPreSchema(example);
        for (FlexibleDatasetSchema.TableField table : schema.getFields()) {
            for (boolean hasSourceTimestamp : new boolean[]{true, false}) {
                for (Optional<Name> alias : new Optional[]{Optional.empty(), Optional.of(tableAlias)}) {
                    FlexibleTableConverter converter = new FlexibleTableConverter(new InputTableSchema(table, hasSourceTimestamp), alias);
                    V visitor = visitorTest.visitorSupplier.get();
                    Optional<T> result = converter.apply(visitor);
                    visitorTest.validator.validate(result,alias.orElse(table.getName()),visitor);
                }
            }
        }

    }



    static class WithSchemaProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments(td -> td.getInputSchema().isPresent());
        }
    }

    static class SchemaConverterProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<SchemaConverter> converters = new ArrayList<>();

            //Calcite
            converters.add(SchemaConverter.of(() -> new CalciteSchemaGenerator(new CalciteTableFactory(new JavaTypeFactoryImpl())),
                    (result, name, calciteVisitor)-> {
                        assertTrue(result.isPresent());
                        RelDataType table = result.get();
                        assertTrue(table.getFieldCount()>0);
                        AbstractTableFactory.UniversalTableBuilder tbl = calciteVisitor.getRootTable();
                        assertTrue(tbl.getAllFields().size()>0);
                        System.out.println(table);
                    }));
            //Flink
            converters.add(SchemaConverter.of(() -> new FlinkTypeInfoSchemaGenerator(),
                    (result, name, visitor)-> {
                        assertTrue(result.isPresent());
                        TypeInformation table = result.get();
                        assertTrue(table.isTupleType());
                        System.out.println(table);
                    }));
            converters.add(SchemaConverter.of(() -> new FlinkTableSchemaGenerator(),
                    (result, name, flinkTable)-> {
                        assertFalse(result.isPresent());
                        org.apache.flink.table.api.Schema schema = flinkTable.getSchema();
                        assertTrue(schema.getColumns().size()>0);
                        System.out.println(schema);
                    }));

            return TestDataset.generateAsArguments(td -> td.getInputSchema().isPresent(), converters);
        }
    }

    @Value
    static class SchemaConverter<T, V extends FlexibleTableConverter.Visitor<T>> {
        Supplier<V> visitorSupplier;
        SchemaConverterValidator<T,V> validator;

        static <T, V extends FlexibleTableConverter.Visitor<T>> SchemaConverter<T,V> of(
                                    Supplier<V> visitorSupplier,
                                    SchemaConverterValidator<T,V> validator) {
            return new SchemaConverter<>(visitorSupplier,validator);
        }
    }

    @FunctionalInterface
    static interface SchemaConverterValidator<T, V extends FlexibleTableConverter.Visitor<T>> {

        void validate(Optional<T> result, Name tableName, V visitor);

    }


}
