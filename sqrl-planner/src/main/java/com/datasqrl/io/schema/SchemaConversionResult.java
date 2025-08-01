package com.datasqrl.io.schema;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;

public record SchemaConversionResult(RelDataType type, Map<String, String> connectorOptions) {}
