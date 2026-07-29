/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.deployment.model.FlinkPlanModel;
import com.datasqrl.deployment.model.JdbcPlanModel;
import com.datasqrl.deployment.model.JdbcStatementModel;
import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.deployment.model.KafkaNewTopicModel;
import com.datasqrl.engine.database.CombinedEnginePlan;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.GenericJdbcStatement;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.log.kafka.KafkaNewTopic;
import com.datasqrl.engine.log.kafka.KafkaPhysicalPlan;
import com.datasqrl.util.SqrlObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/** Guards the explicit mappings from planner state to deployment-file models. */
class PlanModelSerializationTest {

  @Test
  void givenJdbcPhysicalPlan_whenMapped_thenReturnsJdbcPlanModel() {
    var field = new JdbcStatement.Field("id", "BIGINT", false, "primary key");
    var createTable =
        new CreateTableJdbcStatement(
            "orders",
            "order storage",
            List.of(field),
            List.of("id"),
            List.of("tenant"),
            JdbcStatementModel.PartitionType.HASH,
            4,
            Duration.ofSeconds(30),
            null,
            statement -> "CREATE TABLE orders");
    var view =
        new GenericJdbcStatement(
            "orders_view",
            Type.VIEW,
            "CREATE VIEW orders_view",
            "order projection",
            List.of(field));
    var extension =
        new GenericJdbcStatement(
            "pg_cron", Type.EXTENSION, "CREATE EXTENSION pg_cron", "scheduler", List.of());
    var plan =
        JdbcPhysicalPlan.builder()
            .statement(createTable)
            .statement(view)
            .standaloneExtensionStatement(extension)
            .tableIdMap(Map.of())
            .build();

    JdbcPlanModel model = plan.toFileModel();

    assertThat(model.statements()).hasSize(2);
    var table = model.statements().get(0);
    assertThat(table.name()).isEqualTo("orders");
    assertThat(table.type()).isEqualTo(JdbcStatementModel.Type.TABLE);
    assertThat(table.sql()).isEqualTo("CREATE TABLE orders");
    assertThat(table.description()).isEqualTo("order storage");
    assertThat(table.fields())
        .containsExactly(new JdbcStatementModel.Field("id", "BIGINT", false, "primary key"));
    assertThat(table.primaryKey()).containsExactly("id");
    assertThat(table.partitionKey()).containsExactly("tenant");
    assertThat(table.partitionType()).isEqualTo(JdbcStatementModel.PartitionType.HASH);
    assertThat(table.numPartitions()).isEqualTo(4);
    assertThat(table.ttl()).isEqualTo(Duration.ofSeconds(30));

    var viewModel = model.statements().get(1);
    assertThat(viewModel.name()).isEqualTo("orders_view");
    assertThat(viewModel.type()).isEqualTo(JdbcStatementModel.Type.VIEW);
    assertThat(viewModel.sql()).isEqualTo("CREATE VIEW orders_view");
    assertThat(viewModel.description()).isEqualTo("order projection");
    assertThat(viewModel.fields())
        .containsExactly(new JdbcStatementModel.Field("id", "BIGINT", false, "primary key"));
    assertThat(viewModel.primaryKey()).isNull();
    assertThat(viewModel.partitionKey()).isNull();
    assertThat(viewModel.partitionType()).isNull();
    assertThat(viewModel.numPartitions()).isNull();
    assertThat(viewModel.ttl()).isNull();

    assertThat(model.standaloneExtensionStatements())
        .containsExactly(
            new JdbcStatementModel(
                "pg_cron",
                JdbcStatementModel.Type.EXTENSION,
                "CREATE EXTENSION pg_cron",
                "scheduler",
                List.of()));
  }

  @Test
  void givenFlinkPhysicalPlan_whenMapped_thenReturnsFlinkPlanModel() {
    var plan =
        FlinkPhysicalPlan.builder()
            .flinkSql(List.of("CREATE TABLE x"))
            .connectors(Set.of("kafka"))
            .formats(Set.of("json"))
            .functions(Set.of("CREATE FUNCTION f"))
            .build();

    FlinkPlanModel model = plan.toFileModel();

    assertThat(model.flinkSql()).containsExactly("CREATE TABLE x");
    assertThat(model.connectors()).containsExactly("kafka");
    assertThat(model.formats()).containsExactly("json");
    assertThat(model.functions()).containsExactly("CREATE FUNCTION f");
  }

  @Test
  void givenKafkaPhysicalPlan_whenMapped_thenReturnsWrappedTopicModel() {
    var topic = new KafkaNewTopicModel("orders", "orders", 3, (short) 2);
    var testRunnerTopic = new KafkaNewTopicModel("test-orders", "test-orders");
    var plan =
        KafkaPhysicalPlan.builder()
            .topic(new KafkaNewTopic(topic))
            .testRunnerTopic(new KafkaNewTopic(testRunnerTopic))
            .build();

    var model = plan.toFileModel();

    assertThat(model.topics()).containsExactly(topic);
    assertThat(model.testRunnerTopics()).containsExactly(testRunnerTopic);
    assertThat(topic.format()).isNull();
    assertThat(topic.numPartitions()).isEqualTo(3);
    assertThat(topic.replicationFactor()).isEqualTo((short) 2);
    assertThat(topic.type()).isEqualTo(KafkaNewTopicModel.Type.SUBSCRIPTION);
    assertThat(topic.messageKeys()).isEmpty();
    assertThat(topic.messageSchema()).isEmpty();
    assertThat(topic.config()).isEmpty();
    assertThat(testRunnerTopic.numPartitions()).isEqualTo(1);
    assertThat(testRunnerTopic.replicationFactor()).isEqualTo((short) 1);

    var json = SqrlObjectMapper.INSTANCE.valueToTree(model);

    assertThat(List.copyOf(json.properties()))
        .extracting(Map.Entry::getKey)
        .containsExactly("topics", "testRunnerTopics");
  }

  @Test
  void givenCombinedPlanWithCalciteType_whenSerialized_thenUsesNestedFileModels() {
    var typeFactory = new JavaTypeFactoryImpl();
    var statement =
        new GenericJdbcStatement(
            "orders",
            Type.TABLE,
            "CREATE TABLE orders",
            null,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            List.of(new JdbcStatement.Field("id", "BIGINT", false, null)));
    var jdbcPlan = JdbcPhysicalPlan.builder().statement(statement).tableIdMap(Map.of()).build();
    var plan = CombinedEnginePlan.builder().plan("postgres", jdbcPlan).build();

    var json = SqrlObjectMapper.INSTANCE.valueToTree(plan.toFileModel());

    assertThat(json.at("/plans/postgres/statements/0/fields/0/type").asText()).isEqualTo("BIGINT");
  }
}
