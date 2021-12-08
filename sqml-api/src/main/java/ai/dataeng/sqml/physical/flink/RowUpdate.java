package ai.dataeng.sqml.physical.flink;

import com.google.common.base.Preconditions;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

public interface RowUpdate extends Serializable {

    Type getType();

    Row getAddition();

    Row getRetraction();

    default boolean hasAddition() {
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
            if (hasAddition()) s.append(getAddition().toString());
            if (hasRetraction()) s.append(getRetraction().toString());
            s.append("@").append(ingestTime);
            return s.toString();
        }

    }

    class Simple extends Base {
        private static final long serialVersionUID = 7L;

        private final Row add;

        public Simple(Instant ingestTime, Row add) {
            super(ingestTime);
            Preconditions.checkNotNull(add);
            this.add = add;
        }

        public Simple(Instant ingestTime, Object... values) {
            this(ingestTime,new Row(values));
        }

        @Override
        public Type getType() {
            return Type.INSERT;
        }

        @Override
        public Row getAddition() {
            return add;
        }

        @Override
        public Row getRetraction() {
            return null;
        }

    }

    class Full extends Base {
        private static final long serialVersionUID = 7L;

        private final Row add;
        private final Row retract;

        public Full(Instant ingestTime, @Nullable Row add, @Nullable Row retract) {
            super(ingestTime);
            Preconditions.checkArgument(!(add==null && retract==null));
            this.add = add;
            this.retract = retract;
        }

        @Override
        public Type getType() {
            if (add!=null) {
                if (retract==null) return Type.INSERT;
                else return Type.UPDATE;
            } else return Type.DELETE;
        }

        @Override
        public Row getAddition() {
            return add;
        }

        @Override
        public Row getRetraction() {
            return retract;
        }
    }

}
