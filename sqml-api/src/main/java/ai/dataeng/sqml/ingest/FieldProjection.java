package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.type.IntegerType;
import ai.dataeng.sqml.type.ScalarType;
import ai.dataeng.sqml.type.UuidType;
import lombok.Value;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

public interface FieldProjection extends Serializable {

    DestinationTableSchema.Field getField(SourceTableSchema.Table table);

    Object getData(Map<String, Object> data);

    @Value
    class SpecialCase implements FieldProjection {

        private final String name;
        private final ScalarType type;

        @Override
        public DestinationTableSchema.Field getField(SourceTableSchema.Table table) {
            return DestinationTableSchema.Field.primaryKey(name, type);
        }

        @Override
        public Object getData(Map<String, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    FieldProjection ROOT_UUID = new SpecialCase("_uuid", UuidType.INSTANCE);

    FieldProjection ARRAY_INDEX = new SpecialCase("_idx", IntegerType.INSTANCE);

    @Value
    class NamePath implements FieldProjection {

        private final ai.dataeng.sqml.ingest.NamePath path;

        public NamePath(ai.dataeng.sqml.ingest.NamePath path) {
            Preconditions.checkArgument(path.getNumComponents()>0);
            this.path = path;
        }

        @Override
        public DestinationTableSchema.Field getField(SourceTableSchema.Table table) {
            SourceTableSchema.Field sourceField = table.getElement(path).asField();
            Preconditions.checkArgument(!sourceField.isArray(),"A primary key projection cannot be of type ARRAY");
            return DestinationTableSchema.Field.primaryKey(path.toString('_'),sourceField.getType());
        }

        @Override
        public Object getData(Map<String, Object> data) {
            Map<String, Object> base = data;
            for (int i = 0; i < path.getNumComponents()-2; i++) {
                Object map = base.get(path.getComponent(i));
                Preconditions.checkArgument(map instanceof Map, "Illegal field projection");
                base = (Map)map;
            }
            return base.get(path.getLastComponent());
        }
    }

}
