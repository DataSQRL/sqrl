package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import ai.dataeng.sqml.schema2.constraint.NotNull;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public interface FieldProjection extends Serializable {

    int getDepth();

    default Object getData(SourceRecord<Name> record) {
        return getData(record.getData());
    }

    Object getData(Map<Name, Object> data);

    abstract class SpecialCase implements FieldProjection {

        private final Name name;
        private final BasicType type;
        private final boolean isPrimary;

        protected SpecialCase(String name, BasicType type, boolean isPrimary) {
            this.name = Name.hidden(name);
            this.type = type;
            this.isPrimary = isPrimary;
        }

        @Override
        public int getDepth() {
            return 0;
        }

        public Name getName() {
            return name;
        }

        public LogicalPlan.Column createColumn(LogicalPlan.Table table) {
            return new LogicalPlan.Column(getName(),table,0,type,0,
                    Collections.singletonList(NotNull.INSTANCE),isPrimary, false);
        }

        @Override
        public abstract Object getData(SourceRecord<Name> record);

        @Override
        public Object getData(Map<Name, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    SpecialCase ROOT_UUID = new SpecialCase("uuid", UuidType.INSTANCE, true) {
        @Override
        public Object getData(SourceRecord<Name> record) {
            return record.getUuid().toString();
        }
    };

    SpecialCase INGEST_TIMESTAMP = new SpecialCase("ingest_time", DateTimeType.INSTANCE, false) {
        @Override
        public Object getData(SourceRecord<Name> record) {
            return record.getIngestTime();
        }
    };

    SpecialCase SOURCE_TIMESTAMP = new SpecialCase("source_time", DateTimeType.INSTANCE, false) {
        @Override
        public Object getData(SourceRecord<Name> record) {
            return record.getSourceTime();
        }
    };

    @Getter
    public static class ArrayIndex extends SpecialCase {

        private final int depth;

        public ArrayIndex(int depth) {
            super("idx"+depth, IntegerType.INSTANCE, true);
            this.depth = depth;
        }

        @Override
        public Object getData(SourceRecord<Name> record) {
            throw new UnsupportedOperationException();
        }
    }

    class NamePathProjection implements FieldProjection {

        /**
         * The named path is starts at the depth and is relative
         */
        private final NamePath path;
        private final int depth;

        public NamePathProjection(NamePath path, int depth) {
            Preconditions.checkArgument(path.getLength()>0);
            this.path = path;
            this.depth = depth;
        }

        @Override
        public int getDepth() {
            return depth;
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
