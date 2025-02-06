package com.datasqrl.sql;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.SqrlSchema;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.schema.NestedRelationship;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class DatabaseQueryFactory {
  SqrlFramework framework;

  public List<APIQuery> generateQueries(SqrlSchema schema) {
    var i = new AtomicInteger();

    return schema.getTableFunctions()
        .stream().filter(f -> !(f instanceof NestedRelationship))
        .filter(f->!f.getDisplayName().startsWith(HIDDEN_PREFIX))
        .map(t -> new APIQuery(
            t.getDisplayName(),
            t.getFullPath(),
            framework.getQueryPlanner().expandMacros(t.getViewTransform().get()),
            t.getParameters().stream()
                .map(p -> new ArgumentParameter(p.getName()))
                .collect(Collectors.toList()),
            List.of(),
            false
        ))
        .collect(Collectors.toList());
  }
}
