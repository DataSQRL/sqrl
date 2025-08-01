package com.datasqrl.discovery.preprocessor;

import com.datasqrl.io.schema.TableSchemaFactory;
import com.google.auto.service.AutoService;
import java.util.Set;

@AutoService(TableSchemaFactory.class)
public class JsonlDiscoveryTableSchemaFactory extends AbstractDiscoveryTableSchemaFactory {

  public static final String SCHEMA_TYPE = "jsonl";
  public static final Set<String> SCHEMA_EXTENSION = Set.of(SCHEMA_TYPE);

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }

  @Override
  public Set<String> getExtensions() {
    return SCHEMA_EXTENSION;
  }
}
