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
package ai.dataeng.sqml.analyzer;

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Model;
import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.Statement;
import java.util.List;
import java.util.Optional;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final Session session;
    private final List<Expression> parameters;

    public Analyzer(Session session,
            Metadata metadata,
            SqlParser sqlParser,
            List<Expression> parameters)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.parameters = parameters;
    }

    public Analysis analyze(Statement statement,
        QualifiedName name)
    {
//        Statement rewrittenStatement = StatementRewrite
//            .rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters, accessControl, warningCollector);
        Analysis analysis = new Analysis(statement, parameters, name);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, session);
        analyzer.analyze(statement, Optional.empty());

        return analysis;
    }

}
