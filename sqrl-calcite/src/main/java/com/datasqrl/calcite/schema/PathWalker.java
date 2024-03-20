package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class PathWalker {

  private final CatalogReader catalogReader;
  private NamePath path;
  private NamePath absolutePath;
  private NamePath userDefined;

  public PathWalker(CatalogReader catalogReader) {
    this.catalogReader = catalogReader;
    this.path = NamePath.ROOT;
    this.absolutePath = NamePath.ROOT;
    this.userDefined = NamePath.ROOT;
  }

  public PathWalker setPath(NamePath path) {
    this.path = catalogReader.getSqrlAbsolutePath(path);
    this.absolutePath = catalogReader.getSqrlAbsolutePath(path);
    this.userDefined = path;
    return this;
  }

  public void walk(Name next) {
    this.path = absolutePath.concat(next);
    this.absolutePath = catalogReader.getSqrlAbsolutePath(this.path);
    this.userDefined = this.userDefined.concat(next);
  }
}
