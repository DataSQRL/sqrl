package com.datasqrl.plan.queries;

import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class APIQuery {

    private final String nameId;
    private final RelNode relNode;

}
