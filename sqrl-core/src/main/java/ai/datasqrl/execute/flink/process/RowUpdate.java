package ai.datasqrl.execute.flink.process;

import static ai.datasqrl.parse.tree.name.Name.SELF_IDENTIFIER;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.time.Instant;
import javax.annotation.Nullable;

public interface RowUpdate extends Serializable {

    Type getType();

    Row getAppend();

    Row getRetraction();

    default boolean hasAppend() {
        return getType()!=Type.DELETE;
    }

    default boolean hasRetraction() {
        return getType()!=Type.INSERT;
    }

    Instant getIngestTime();


    enum Type {
        INSERT("I"), UPDATE("U"), DELETE("D");

        private final String prefix;

        Type(String prefix) {
            this.prefix = prefix;
        }
    }

    abstract class Base implements RowUpdate {

        private final Instant ingestTime;

        protected Base(Instant ingestTime) {
            this.ingestTime = ingestTime;
            Preconditions.checkNotNull(ingestTime);
        }

        @Override
        public Instant getIngestTime() {
            return ingestTime;
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append(getType().prefix);
            if (hasAppend()) s.append(getAppend().toString());
            if (hasRetraction()) s.append(getRetraction().toString());
            s.append(SELF_IDENTIFIER).append(ingestTime);
            return s.toString();
        }

    }

    class AppendOnly extends Base {
        private static final long serialVersionUID = 7L;

        private final Row append;

        public AppendOnly(Instant ingestTime, Row append) {
            super(ingestTime);
            Preconditions.checkNotNull(append);
            this.append = append;
        }

        public AppendOnly(RowUpdate ru, Row append) {
            this(ru.getIngestTime(), append);
        }

        public AppendOnly(Instant ingestTime, Object... values) {
            this(ingestTime,new Row(values));
        }

        @Override
        public Type getType() {
            return Type.INSERT;
        }

        @Override
        public Row getAppend() {
            return append;
        }

        @Override
        public Row getRetraction() {
            return null;
        }

    }

    class Full extends Base {
        private static final long serialVersionUID = 7L;

        private final Row append;
        private final Row retract;

        public Full(Instant ingestTime, @Nullable Row append, @Nullable Row retract) {
            super(ingestTime);
            Preconditions.checkArgument(!(append==null && retract==null));
            this.append = append;
            this.retract = retract;
        }

        public Full(RowUpdate ru, @Nullable Row add, @Nullable Row retract) {
            this(ru.getIngestTime(),add,retract);
        }


        @Override
        public Type getType() {
            if (append !=null) {
                if (retract==null) return Type.INSERT;
                else return Type.UPDATE;
            } else return Type.DELETE;
        }

        @Override
        public Row getAppend() {
            return append;
        }

        @Override
        public Row getRetraction() {
            return retract;
        }
    }

}
