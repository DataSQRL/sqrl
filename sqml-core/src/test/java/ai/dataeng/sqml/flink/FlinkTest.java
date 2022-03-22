package ai.dataeng.sqml.flink;

import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.planner.operator.C360Test;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.FunctionHints;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.functions.SqlUnnestUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;

public class FlinkTest {

    @SneakyThrows
    @Test
    public void testJSONBookTable() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        TableDescriptor jsonTable = TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("title", DataTypes.STRING())
                        .build())
                .option("path", ConfigurationTest.DATA_DIR.toAbsolutePath() + "/book_001.json")
                .format("json")
                .build();



        tEnv.createTable("book",jsonTable);

        Table book = tEnv.from("book");

        Table sum = book.select($("id").sum().as("sum"));

        tEnv.toChangelogStream(sum).print();

        env.execute();
    }

    @SneakyThrows
    @Test
    public void testJSONOrderTable() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataType entriesArray = DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("productid", DataTypes.INT()),
                DataTypes.FIELD("quantity", DataTypes.INT())));
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("customerid", DataTypes.INT())
                .column("time", DataTypes.INT())
                .column("entries", entriesArray)
                .build();


        TableDescriptor jsonTable = TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", C360Test.RETAIL_DATA_DIR.toAbsolutePath() + "/orders.json")
                .format("json")
                .build();

//        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");
//        tEnv.fromDataStream(dataStream,schema);

        tEnv.createTable("Orders",jsonTable);
        Table orders = tEnv.from("Orders");
        TableFunction unnestIdx = createUnnestIndexFct(entriesArray);
        tEnv.createTemporarySystemFunction("unnestIdx", unnestIdx);
//        Table res = orders.flatMap(call("flattenEntries", $("entries")));

        Table flattenEntries = orders.joinLateral(call("unnestIdx", $("entries")
                                                        ))
                            .select($("id"),$("productid"),$("quantity"),$("_idx"));

//        Table shreddedEntries = tEnv.sqlQuery("SELECT  o.id, o.customerid, items.productid, items.quantity \n" +
//                "FROM Orders o CROSS JOIN UNNEST(o.entries) AS items (productid, quantity)");



        tEnv.toChangelogStream(flattenEntries).print();

        env.execute();
    }

    public static SqlUnnestUtils.CollectionUnnestTableFunction createUnnestIndexFct(DataType datatype) {
        LogicalType logicalType = datatype.getLogicalType();
        Preconditions.checkArgument(logicalType.getTypeRoot()== LogicalTypeRoot.ARRAY);
        ArrayType arrayType = (ArrayType) logicalType;
        Preconditions.checkArgument(arrayType.getElementType().getTypeRoot() == LogicalTypeRoot.ROW);

        RowType rowType = (RowType) arrayType.getElementType();
        final int fieldcount = rowType.getFieldCount();
        final RowData.FieldGetter[] getters = new RowData.FieldGetter[fieldcount];
        List<RowType.RowField> fieldsPlusIndex = new ArrayList<>(fieldcount+1);
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fieldcount; i++) {
            RowType.RowField field = fields.get(i);
            fieldsPlusIndex.add(i,field);
            getters[i]=RowData.createFieldGetter(field.getType(),i);
        }
        fieldsPlusIndex.add(new RowType.RowField("_idx", DataTypes.INT().getLogicalType(), "Index of unnested array"));
        RowType rowTypeIndex = new RowType(fieldsPlusIndex);

        ArrayData.ElementGetter elementGetter = (array, pos) -> {
            RowData data = array.getRow(pos, fieldcount);
            GenericRowData indexedData = new GenericRowData(data.getRowKind(),fieldcount+1);
            for (int i = 0; i < fieldcount; i++) {
                indexedData.setField(i,getters[i].getFieldOrNull(data));
            }
            indexedData.setField(fieldcount,pos);
            return indexedData;
        };

        return new SqlUnnestUtils.CollectionUnnestTableFunction(
                arrayType,
                rowTypeIndex,
                elementGetter);

    }



}
