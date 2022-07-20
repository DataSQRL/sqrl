package ai.datasqrl.schema.input;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.builder.TableBuilder;
import ai.datasqrl.schema.type.SqrlTypeVisitor;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.*;

public abstract class TableBuilderFlexibleTableConverterVisitor<T,X extends TableBuilder<T,X>> implements FlexibleTableConverter.Visitor<T> {

    public final TableBuilder.Factory<T,X> tableFactory;
    public final Deque<X> stack = new ArrayDeque<>();

    @Getter
    private X lastCreatedTable = null;

    protected TableBuilderFlexibleTableConverterVisitor(TableBuilder.Factory<T, X> tableFactory) {
        this.tableFactory = tableFactory;
    }

    public X getRootTable() {
        Preconditions.checkArgument(stack.isEmpty() && lastCreatedTable != null);
        return lastCreatedTable;
    }

    @Override
    public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton, boolean hasSourceTimestamp) {
        if (isNested) {
            assert stack.getFirst()!=null;
            stack.addFirst(tableFactory.createTable(name,namePath, stack.getFirst(), isSingleton));
        } else {
            stack.addFirst(tableFactory.createTable(name,namePath));
        }
        augmentTable(isNested, isSingleton, hasSourceTimestamp);
    }

    protected void augmentTable(boolean isNested, boolean isSingleton, boolean hasSourceTimestamp) {
        if (!isNested) {
            addField(ReservedName.UUID,UuidType.INSTANCE,true);
            addField(ReservedName.INGEST_TIME,DateTimeType.INSTANCE,true);
            if (hasSourceTimestamp) {
                addField(ReservedName.SOURCE_TIME, DateTimeType.INSTANCE, true);
            }
        }
        if (isNested && !isSingleton) {
            addField(ReservedName.ARRAY_IDX, IntegerType.INSTANCE, true);
        }
    }

    @Override
    public Optional<T> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
        lastCreatedTable = stack.removeFirst();
        return createTable(lastCreatedTable);
    }

    protected abstract Optional<T> createTable(X tblBuilder);

    @Override
    public void addField(Name name, T type, boolean nullable, boolean isNested, boolean isSingleton) {
        X tblBuilder = stack.getFirst();
        if (isNested) {
            //It's a relationship
            Relationship.Multiplicity multi = Relationship.Multiplicity.ZERO_ONE;
            if (!isSingleton) multi = Relationship.Multiplicity.MANY;
            else if (!nullable) multi = Relationship.Multiplicity.ONE;
            tblBuilder.addChild(name,lastCreatedTable,nullable(type, nullable),multi);
            lastCreatedTable = null;
        } else {
            tblBuilder.addColumn(name,nullable(type, nullable), nullable);
        }
    }

    public abstract T nullable(T type, boolean nullable);

    public T convertBasicType(BasicType type) {
        return type.accept(getTypeConverter(),null);
    }

    protected abstract SqrlTypeConverter<T> getTypeConverter();

    public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T,Void> {

    }

}
