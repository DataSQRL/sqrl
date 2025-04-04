
import org.apache.flink.table.api.*;

public class FlinkPlanGenerator {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Define source table
        tEnv.executeSql("""
            CREATE TABLE source_table (
              id INT,
              name STRING
            ) WITH (
              'connector' = 'datagen',
              'rows-per-second' = '1000',
              'fields.id.kind' = 'sequence',
              'fields.id.start' = '1',
              'fields.id.end' = '13'
            )
        """);

        // Define sink table
        tEnv.executeSql("""
CREATE TABLE sink_table (
  text STRING
) WITH (
  'connector' = 'print'
);
        """);

        // Register the query
        Table result = tEnv.sqlQuery("SELECT concat('Completed ID: ', CAST(id as String), ' name: ', name) FROM source_table");

        // Insert into sink
        tEnv.createStatementSet()
            .addInsert("sink_table", result)
            .explain(); // For debug

        // Compile the plan
        CompiledPlan plan = tEnv.createStatementSet()
            .addInsert("sink_table", result)
            .compilePlan();

        // Save to file
        java.nio.file.Files.writeString(
            java.nio.file.Paths.get("concat.plan"),
            plan.asJsonString()
        );

        System.out.println("✅ Plan written to compiled.plan");
    }
}
