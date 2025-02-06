package com.datasqrl.function;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.google.common.collect.Maps;

public class AbstractFunctionModule implements SqrlModule {

  private final List<NamespaceObject> functions;

  private Map<Name, NamespaceObject> functionMap;

  public AbstractFunctionModule(List<NamespaceObject> functions) {
    this.functions = functions;
    this.functionMap = Maps.uniqueIndex(functions, NamespaceObject::getName);
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.ofNullable(functionMap.get(name));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return functions;
  }
}
