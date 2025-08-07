package com.datasqrl.engine.database.relational.ddl.statements;

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;

import com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory;
import java.util.List;
import java.util.stream.Collectors;

public class GenericCreateViewDdlFactory {

  public String createView(String viewName, List<String> columns, String select) {
    return "CREATE OR REPLACE VIEW %s (%s) AS %s"
        .formatted(
            quoteIdentifier(viewName),
            columns.stream()
                .map(AbstractJdbcStatementFactory::quoteIdentifier)
                .collect(Collectors.joining(", ")),
            select);
  }
}
