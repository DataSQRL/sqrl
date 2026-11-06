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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.deployment.model.JdbcPlanModel;
import com.datasqrl.deployment.model.JdbcStatementModel;
import com.datasqrl.util.SqrlObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Checks the tags in the database deployment plan, which the snapshot of the {@code tags-compile}
 * use case does not include.
 */
class TagDeploymentAssetsTest {

  @RegisterExtension final CliCompileTestExtension compiler = new CliCompileTestExtension();

  @Test
  void givenTaggedTables_whenCompiled_thenDatabasePlanListsTheirTags() throws Exception {
    var hook =
        compiler.execute(getResourcesDirectory("usecases/tags-compile"), "compile", "package.json");
    assertThat(hook.isFailed()).as(hook.getMessages()).isFalse();

    var plan =
        SqrlObjectMapper.INSTANCE.readValue(
            compiler.getPlanDir().resolve("postgres.json").toFile(), JdbcPlanModel.class);
    var tags =
        plan.statements().stream()
            .filter(statement -> statement.type() == JdbcStatementModel.Type.TABLE)
            .collect(
                Collectors.toMap(
                    JdbcStatementModel::name,
                    statement -> statement.tags() == null ? Map.of() : statement.tags()));

    assertThat(tags)
        // passed through by DISTINCT, kept by a column expression, declared on a column
        .containsEntry("Customers", Map.of("classification", "confidential", "pii", "raw"))
        .containsEntry(
            "ApplicationsStream", Map.of("classification", "internal", "financial", "true"))
        .containsEntry(
            "CustomerContacts",
            Map.of("classification", "confidential", "gdpr", "true", "pii", "raw"))
        // declassified
        .containsEntry("LoanTotals", Map.of("classification", "confidential", "financial", "true"))
        .containsEntry("CustomerNameCounts", Map.of());
  }
}
