package com.datasqrl.v2.analyzer;

import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  ObjectIdentifier getIdentifier();

}
