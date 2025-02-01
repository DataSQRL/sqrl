package com.datasqrl.function;

import com.datasqrl.module.SqrlModule;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.NamespaceObject;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class AbstractFunctionModule implements SqrlModule {

  private final List<FlinkUdfNsObject> functions;

  private Map<Name, FlinkUdfNsObject> functionMap;

  public AbstractFunctionModule(List<FlinkUdfNsObject> functions) {
    this.functions = functions;
    this.functionMap = Maps.uniqueIndex(functions, NamespaceObject::getName);
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.ofNullable(functionMap.get(name));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return (List)functions;
  }
}
