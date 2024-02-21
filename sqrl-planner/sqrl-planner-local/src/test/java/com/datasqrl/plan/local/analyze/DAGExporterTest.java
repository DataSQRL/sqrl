package com.datasqrl.plan.local.analyze;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DAGExporterTest extends AbstractLogicalSQRLIT {

    protected SnapshotTest.Snapshot snapshot;

    private final Deserializer deserializer = new Deserializer();

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

    @Test
    public void joinTimestampPropagationTest() {
        ScriptBuilder builder = DAGPlannerTest.imports(false);
        builder.add("OrderCustomer1 := SELECT o.id, c.name FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer2 := SELECT o.id, c.name, GREATEST(o._ingest_time, c._ingest_time) AS timestamp FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer3 := SELECT o.id, c.name, p.name FROM Orders o JOIN Customer c on o.customerid = c.customerid JOIN Product p ON p.productid = c.customerid");
        export(builder, "ordercustomer1", "ordercustomer2", "ordercustomer3");
    }


    /* ======== UTILITY METHODS ======== */

    @SneakyThrows
    private void export(ScriptBuilder script, String... queryTableNames) {
        SqrlDAG dag = super.planDAG(script.getScript(), Arrays.asList(queryTableNames));
        SqrlDAGExporter exporter = SqrlDAGExporter.builder()
                .build();
        List<SqrlDAGExporter.Node> nodes = exporter.export(dag);
        Collections.sort(nodes); //make order deterministic
        String text = nodes.stream().map(SqrlDAGExporter.Node::toString)
                .collect(Collectors.joining("\n"));
        snapshot.addContent(text, "TEXT");
        snapshot.addContent(deserializer.toJson(nodes), "JSON");
    }

}
