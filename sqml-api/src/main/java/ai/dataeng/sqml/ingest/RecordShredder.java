package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.type.IntegerType;
import ai.dataeng.sqml.type.ScalarType;
import ai.dataeng.sqml.type.TypeMapping;
import ai.dataeng.sqml.type.UuidType;
import lombok.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RecordShredder implements FlatMapFunction<SourceRecord, Row> {

    private final NamePath tableIdentifier;
    private final FieldProjection[][] parentProjections;
    private final SourceTableSchema schema;
    private final TimestampSelector timestampSelector;

    private RowTypeInfo flinkType;

    public RecordShredder(NamePath tableIdentifier, FieldProjection[][] parentProjections, SourceTableSchema schema, TimestampSelector timestampSelector) {
        this.timestampSelector = timestampSelector;
        Preconditions.checkArgument(parentProjections.length==tableIdentifier.getNumComponents()+1,"Invalid number of projections provided: %s", parentProjections);
        this.tableIdentifier = tableIdentifier;
        this.parentProjections = parentProjections;
        this.schema = schema;
        this.flinkType = compileTypeInfo();
    }

    public RecordShredder(NamePath tableIdentifier, SourceTableSchema schema) {
        this(tableIdentifier,defaultParentKeys(tableIdentifier,schema),schema, TimestampSelector.SOURCE_TIME);
    }

    private static final FieldProjection[][] defaultParentKeys(NamePath tableIdentifier, SourceTableSchema schema) {
        int depth = tableIdentifier.getNumComponents();
        FieldProjection[][] projs = new FieldProjection[depth+1][];
        for (int i = 0; i < projs.length; i++) {
            if (i==0) {
                projs[i]=new FieldProjection[]{ROOT_UUID};
            } else if (schema.getTable().getElement(tableIdentifier.prefix(i)).asTable().isArray()) {
                projs[i]=new FieldProjection[]{ARRAY_INDEX};
            } else {
                projs[i]=new FieldProjection[0];
            }
        }
        return projs;
    }

    @Override
    public void flatMap(SourceRecord sourceRecord, Collector<Row> collector) throws Exception {
        Object[] cols = new Object[flinkType.getFieldNames().length];
        int[] internalIdx = new int[tableIdentifier.getNumComponents()];
        int colno = 0;
        if (parentProjections[0].length>0 && parentProjections[0][0].equals(ROOT_UUID)) {
            //Special case of using uuid for global key
            Preconditions.checkArgument(sourceRecord.hasUUID());
            cols[colno++]=sourceRecord.getUuid().toString();
        }
        //Add timestamps
        cols[cols.length-1] = timestampSelector.getTimestamp(sourceRecord);
        //Construct rows iteratively
        constructRows(sourceRecord.getData(),cols, colno, 0, collector);
    }

    private void constructRows(Map<String,Object> data, Object[] cols, int colno, int depth,
                               Collector<Row> collector) {
        //Add projections at the current level
        for (FieldProjection proj : parentProjections[depth]) {
            //We can ignore special projections here since those are handled at the outer level
            if (proj instanceof SpecialFieldProjection) continue;
            Preconditions.checkArgument(proj instanceof NamePathProjection);
            cols[colno++]=proj.getData(data);
        }

        if (depth==tableIdentifier.getNumComponents()) {
            //We have reached the most inner table and maximum depth - collect table data and wrap up row
            SourceTableSchema.Table table = schema.getElement(tableIdentifier).asTable();
            Iterator<String> colNames = table.getFields().map(e -> e.getKey()).iterator();
            while(colNames.hasNext()) {
                cols[colno++]=data.get(colNames.next());
            }
            Row row = Row.ofKind(RowKind.INSERT, cols);
            //System.out.println("Shredded row: " + row);
            collector.collect(row);
        } else {
            //We have to get into nested table
            Object nested = data.get(tableIdentifier.getComponent(depth));
            if (nested instanceof Map) {
                constructRows((Map)nested,cols,colno,depth+1,collector);
            } else if (nested.getClass().isArray()) {
                Object[] arr = (Object[])nested;
                boolean addIndex = parentProjections[depth+1].length>0 && parentProjections[depth+1][0].equals(ARRAY_INDEX);
                for (int i = 0; i < arr.length; i++) {
                    Object[] colcopy = cols.clone();
                    if (addIndex) {
                        colcopy[colno]=i;
                    }
                    constructRows((Map)nested,cols.clone(),colno+(addIndex?1:0),depth+1,collector);
                }
            } else throw new IllegalArgumentException("Unexpected data encountered: " + nested);
        }
    }

    public RowTypeInfo getTypeInfo() {
        return flinkType;
    }

    private RowTypeInfo compileTypeInfo() {
        List<TypeInformation> colTypes = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        //Add parent keys
        for (int depth = 0; depth < parentProjections.length; depth ++) {
            FieldProjection[] proj = parentProjections[depth];
            NamePath base = tableIdentifier.prefix(depth);

            for (int projNo = 0; projNo < proj.length; projNo++) {
                String prefix = "";
                if (depth < parentProjections.length-1) prefix = "_parent"+depth+"_"+projNo;
                colNames.add(prefix+proj[projNo].getColumnName());
                colTypes.add(proj[projNo].getColumnType(schema.getElement(base).asTable()));
            }
        }
        //Add table columns
        SourceTableSchema.Table table = schema.getElement(tableIdentifier).asTable();
        table.getFields().forEach(e -> {
            colNames.add(e.getKey());
            colTypes.add(e.getValue().asField().getFlinkTypeInfo());
        });

        //Add Timestamps from SourceRecord
        colTypes.add(BasicTypeInfo.INSTANT_TYPE_INFO);
        colNames.add("__timestamp");

        assert colTypes.size() == colNames.size();

        return new RowTypeInfo(colTypes.toArray(new TypeInformation[0]),colNames.toArray(new String[0]));
    }

    public interface FieldProjection extends Serializable {

        String getColumnName();

        TypeInformation getColumnType(SourceTableSchema.Table table);

        Object getData(Map<String,Object> data);

    }

    @Value
    public static class SpecialFieldProjection implements FieldProjection {

        private final String name;
        private final ScalarType type;

        @Override
        public String getColumnName() {
            return name;
        }

        @Override
        public TypeInformation getColumnType(SourceTableSchema.Table table) {
            return TypeMapping.getFlinkTypeInfo(type,false);
        }

        @Override
        public Object getData(Map<String, Object> data) {
            throw new UnsupportedOperationException();
        }
    }

    public static final FieldProjection ROOT_UUID = new SpecialFieldProjection("_uuid", UuidType.INSTANCE);

    public static final FieldProjection ARRAY_INDEX = new SpecialFieldProjection("_idx", IntegerType.INSTANCE);

    public static class NamePathProjection implements FieldProjection {

        private final NamePath path;

        public NamePathProjection(NamePath path) {
            Preconditions.checkArgument(path.getNumComponents()>0);
            this.path = path;
        }

        @Override
        public String getColumnName() {
            return path.toString('_');
        }

        @Override
        public TypeInformation getColumnType(SourceTableSchema.Table table) {
            return table.getElement(path).asField().getFlinkTypeInfo();
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
