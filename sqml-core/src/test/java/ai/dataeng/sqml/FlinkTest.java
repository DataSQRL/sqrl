package ai.dataeng.sqml;

import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.planner.operator.C360Test;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.FunctionHints;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

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


        TableDescriptor jsonTable = TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("customerid", DataTypes.INT())
                        .column("time", DataTypes.INT())
                        .column("entries", DataTypes.ARRAY(DataTypes.ROW(
                                DataTypes.FIELD("productid", DataTypes.INT()),
                                DataTypes.FIELD("quantity", DataTypes.INT())
//                                DataTypes.FIELD("unit_price", DataTypes.DECIMAL(9,3)),
//                                DataTypes.FIELD("discount", DataTypes.DECIMAL(9,3))
                                )))
                        .build())
                .option("path", C360Test.RETAIL_DATA_DIR.toAbsolutePath() + "/orders.json")
                .format("json")
                .build();



        tEnv.createTable("Orders",jsonTable);
        Table orders = tEnv.from("Orders");
//        Table flattenEntries = orders.select($("entries").at(1).flatten().get("quantity").flatten().sum().as("totalquant"));

        Table shreddedEntries = tEnv.sqlQuery("SELECT  o.id, o.customerid, items.productid, items.quantity \n" +
                "FROM Orders o CROSS JOIN UNNEST(o.entries) AS items (productid, quantity)");

//        TableFunction func = new FlattenFunction();
//        tEnv.createTemporarySystemFunction("flattenEntries", func);
//        Table res = orders.flatMap(call("flattenEntries", $("entries")));

        tEnv.toChangelogStream(shreddedEntries).print();

        env.execute();
    }

    @FunctionHint(
            input = {@DataTypeHint("ARRAY<ROW<productid INT, quantity INT>>")},
            output = @DataTypeHint("ROW<productid INT, quantity INT>")
    )
  public class FlattenFunction extends TableFunction<Row> {
   public void eval(Row... args) {
     for (Row i : args) {
       collect(i);
     }
   }

 }


}
