package com.datasqrl.flinkwrapper.analyzer;

import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  ObjectIdentifier getIdentifier();

}
