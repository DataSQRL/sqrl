package com.datasqrl.graphql.visitor;

import graphql.language.Document;

public interface GraphqlDocumentVisitor<R, C> {
  public R visitDocument(Document node, C context);
}
