package ai.dataeng.sqml.ingest.shredding;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.schema2.*;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.Value;

import java.io.Serializable;
import java.util.Map;

public interface FieldProjection extends Serializable {

    DestinationTableSchema.Field getField(RelationType<StandardField> table);

    Object getData(Map<Name, Object> data);

    @Value
    class SpecialCase implements FieldProjection {

        private final String name;
        private final BasicType type;

        @Override
        public DestinationTableSchema.Field getField(RelationType<StandardField> table) {
            return DestinationTableSchema.Field.primaryKey(name, type);
        }

        @Override
        public Object getData(Map<Name, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    FieldProjection ROOT_UUID = new SpecialCase("_uuid", UuidType.INSTANCE);

    FieldProjection ARRAY_INDEX = new SpecialCase("_idx", IntegerType.INSTANCE);

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
