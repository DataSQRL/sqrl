package com.datasqrl.plan.local.analyze;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.plan.table.TableType;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class DAGPlannerTest extends AbstractLogicalSQRLIT {

    protected SnapshotTest.Snapshot snapshot;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        initialize(IntegrationTestSettings.getInMemory(), null, Optional.empty());
        this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    }

    @AfterEach
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        snapshot.createOrValidate();
    }

    /* ==== IMPORT ==== */

    @Test
    public void importTest() {
        ScriptBuilder s = imports(false);
        validateTables(s, "customer", "orders", "product");
    }

    @Test
    public void importWithNaturalTimestampTest() {
        ScriptBuilder s = imports(true);
        validateTables(s, "customer", "orders", "product");
    }

    /* ==== JOIN === */

    @Test
    public void joinTimestampPropagationTest() {
        ScriptBuilder builder = imports(false);
        builder.add("OrderCustomer1 := SELECT o.id, c.name FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer2 := SELECT o.id, c.name, GREATEST(o._ingest_time, c._ingest_time) AS timestamp FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer3 := SELECT o.id, c.name, p.name FROM Orders o JOIN Customer c on o.customerid = c.customerid JOIN Product p ON p.productid = c.customerid");
        validateTables(builder, "ordercustomer1", "ordercustomer2", "ordercustomer3");
    }


    /* ======== UTILITY METHODS ======== */

    private void validateTables(ScriptBuilder script, String... queryTableNames) {
        SqrlDAG dag = super.planDAG(script.getScript(), Arrays.asList(queryTableNames));
        SqrlDAGExporter exporter = SqrlDAGExporter.builder()
                .includeQueries(false)
                .includeImports(false)
                .build();
        snapshot.addContent(exporter.export(dag).stream().map(SqrlDAGExporter.Node::toString)
                .collect(Collectors.joining("\n")));
    }


    private ScriptBuilder imports(boolean useNaturalTimestamp) {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT time.*");
        builder.append("IMPORT ecommerce-data.Customer  TIMESTAMP " + (useNaturalTimestamp? "epochToTimestamp(lastUpdated) AS timestamp" : "_ingest_time"));
        builder.append("IMPORT ecommerce-data.Orders TIMESTAMP " + (useNaturalTimestamp? "time": "_ingest_time"));
        builder.append("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time");
        return builder;
    }

}
