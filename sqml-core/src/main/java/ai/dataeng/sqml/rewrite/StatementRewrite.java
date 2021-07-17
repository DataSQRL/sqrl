/*
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
package ai.dataeng.sqml.rewrite;

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import java.util.List;

public final class StatementRewrite
{
    private static final List<Rewrite> REWRITES = ImmutableList.of(
    );

    private StatementRewrite() {}

    public static Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Statement node,
            List<Expression> parameters)
    {
        for (Rewrite rewrite : REWRITES) {
            node = requireNonNull(rewrite.rewrite(session, metadata, parser, node, parameters), "Statement rewrite returned null");
        }
        return node;
    }

    interface Rewrite
    {
        Statement rewrite(
                Session session,
                Metadata metadata,
                SqlParser parser,
                Statement node,
                List<Expression> parameters);
    }
}
