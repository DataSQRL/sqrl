package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.sqrl.CatalogResolver;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class PathWalker {

  private final CatalogResolver catalogReader;
  private List<String> path;
  private List<String> absolutePath;
  private List<String> userDefined;

  public PathWalker(CatalogResolver catalogReader) {
    this.catalogReader = catalogReader;
    this.path = new ArrayList<>();
    this.absolutePath = new ArrayList<>();
    this.userDefined = new ArrayList<>();
  }

  public PathWalker setPath(List<String> path) {
    this.path.addAll(catalogReader.getSqrlAbsolutePath(path).toStringList());
    this.absolutePath.addAll(catalogReader.getSqrlAbsolutePath(path).toStringList());
    this.userDefined.addAll(path);
    return this;
  }

  public void walk(String next) {
    this.path.clear();
    this.path.addAll(absolutePath);
    this.path.add(next);
    this.absolutePath.clear();
    this.absolutePath.addAll(catalogReader.getSqrlAbsolutePath(this.path).toStringList());
    this.userDefined.add(next);
  }
}
