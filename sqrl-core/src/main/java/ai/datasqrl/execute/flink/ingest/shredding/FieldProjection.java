package ai.datasqrl.execute.flink.ingest.shredding;

import ai.datasqrl.execute.flink.process.DestinationTableSchema;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.type.RelationType;
import ai.datasqrl.schema.type.StandardField;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.TypeHelper;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Map;
import lombok.Value;

public interface FieldProjection extends Serializable {

    DestinationTableSchema.Field getField(RelationType<StandardField> table);

    Object getData(Map<Name, Object> data);

    @Value
    class SpecialCase implements FieldProjection {

        private final ReservedName name;
        private final BasicType type;

        @Override
        public DestinationTableSchema.Field getField(RelationType<StandardField> table) {
            return DestinationTableSchema.Field.primaryKey(name.getCanonical(), type);
        }

        @Override
        public Object getData(Map<Name, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    FieldProjection ROOT_UUID = new SpecialCase(ReservedName.UUID, UuidType.INSTANCE);
    FieldProjection INGEST_TIME = new SpecialCase(ReservedName.INGEST_TIME, DateTimeType.INSTANCE);

    FieldProjection ARRAY_INDEX = new SpecialCase(ReservedName.ARRAY_IDX, IntegerType.INSTANCE);

    @Value
    class NamePathProjection implements FieldProjection {

        private final NamePath path;

        public NamePathProjection(NamePath path) {
            Preconditions.checkArgument(path.getLength()>0);
            this.path = path;
        }

        @Override
        public DestinationTableSchema.Field getField(RelationType<StandardField> table) {
            Type type = TypeHelper.getNestedType(table,path);
            Preconditions.checkArgument(type instanceof BasicType,"A primary key projection must be of basic type: %s", type);
            return DestinationTableSchema.Field.primaryKey(path.toString('_'), (BasicType) type);
        }

        @Override
        public Object getData(Map<Name, Object> data) {
            Map<Name, Object> base = data;
            for (int i = 0; i < path.getLength()-2; i++) {
                Object map = base.get(path.get(i));
                Preconditions.checkArgument(map instanceof Map, "Illegal field projection");
                base = (Map)map;
            }
            return base.get(path.getLast());
        }
    }

}
