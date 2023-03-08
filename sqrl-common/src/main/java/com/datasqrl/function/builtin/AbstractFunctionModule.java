package com.datasqrl.function.builtin;

import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
