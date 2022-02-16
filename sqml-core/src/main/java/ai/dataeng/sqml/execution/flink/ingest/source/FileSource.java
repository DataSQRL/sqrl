package ai.dataeng.sqml.execution.flink.ingest.source;

import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.io.sources.impl.file.FileTableConfiguration;
import ai.dataeng.sqml.io.sources.impl.file.FileType;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Is this still needed?
 */
public class FileSource {

    private TypeInformation<Row> toTypeInformation(FileTableConfiguration table, List<FlexibleDatasetSchema.FlexibleField> fieldList) {
        List<String> names = new ArrayList<>();
        List<TypeInformation> types = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            FlexibleDatasetSchema.FlexibleField field = fieldList.get(i);
            if (field.getType() instanceof RelationType) {
                continue;
            }
            names.add(field.getCanonicalName());
            if (table.getFileType() == FileType.CSV) {
                types.add(Types.STRING);//toType(field.getTypes().get(0).getType());
            } else {
                types.add(toType(field.getTypes().get(0).getType()));
            }
        }
        names.add("__uuid");
        types.add(TypeInformation.of(UUID.class));

        return Types.ROW_NAMED(
                names.toArray(new String[0]),
                types.toArray(new TypeInformation[0]));
    }

    private TypeInformation toType(Type type) {
        return type.accept(new SqmlTypeVisitor<>() {
            @Override
            public TypeInformation visitBooleanType(BooleanType type, Object context) {
                return Types.BOOLEAN;
            }

            @Override
            public TypeInformation visitFloatType(FloatType type, Object context) {
                return Types.FLOAT;
            }

            @Override
            public TypeInformation visitIntegerType(IntegerType type, Object context) {
                return Types.INT;
            }

            @Override
            public TypeInformation visitStringType(StringType type, Object context) {
                return Types.STRING;
            }

            @Override
            public TypeInformation visitDateTimeType(DateTimeType type, Object context) {
                return Types.INSTANT;
            }

            @Override
            public TypeInformation visitUuidType(UuidType type, Object context) {
                return TypeInformation.of(UUID.class);
            }
        }, null);
    }




}
