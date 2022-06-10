package ai.datasqrl.schema.input;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.type.SqrlTypeVisitor;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public abstract class AbstractFlexibleTableConverterVisitor<T> implements FlexibleTableConverter.Visitor<T> {

    public final Deque<TableBuilder<T>> stack = new ArrayDeque<>();

    @Override
    public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
        stack.addFirst(new TableBuilder<>(name, namePath));
    }

    protected void augmentTable(boolean isNested, boolean isSingleton) {
        if (!isNested) {
            addField(ReservedName.UUID,UuidType.INSTANCE,true);
            addField(ReservedName.INGEST_TIME,DateTimeType.INSTANCE,true);
            addField(ReservedName.SOURCE_TIME,DateTimeType.INSTANCE,false);
        }
        if (isNested && !isSingleton) {
            addField(ReservedName.ARRAY_IDX, IntegerType.INSTANCE, true);
        }
    }

    @Override
    public Optional<T> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
        augmentTable(isNested, isSingleton);
        return createTable(stack.removeFirst());
    }

    protected abstract Optional<T> createTable(TableBuilder<T> tblBuilder);

    @Override
    public void addField(Name name, T type, boolean notnull) {
        stack.getFirst().add(name,nullable(type,notnull));
    }

    public abstract T nullable(T type, boolean notnull);

    public T convertBasicType(BasicType type) {
        return type.accept(getTypeConverter(),null);
    }

    protected abstract SqrlTypeConverter<T> getTypeConverter();

    protected interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T,Void> {

    }

    @Value
    protected static class TableBuilder<T> {

        final Name name;
        final NamePath namePath;
        final List<Pair<Name,T>> columns = new ArrayList<>();

        void add(Name name, T type) {
            columns.add(Pair.of(name,type));
        }

    }

}
