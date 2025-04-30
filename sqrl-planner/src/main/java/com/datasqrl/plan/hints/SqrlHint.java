/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.tools.RelBuilder;

import lombok.NonNull;

public interface SqrlHint {

  RelHint getHint();

  String getHintName();

  default RelNode addHint(Hintable node) {
    return node.attachHints(List.of(getHint()));
  }

  default RelBuilder addTo(RelBuilder relBuilder) {
    return relBuilder.hints(getHint());
  }

  static <H extends SqrlHint> Optional<H> fromRel(RelNode node,
      SqrlHint.Constructor<H> hintConstructor) {
    if (node instanceof Hintable) {
      return ((Hintable) node).getHints().stream()
          .filter(h -> hintConstructor.validName(h.hintName))
          .filter(
              h -> h.inheritPath.isEmpty()) //we only want the hint on that particular node, not inherited ones
          .findFirst().map(hintConstructor::fromHint);
    }
    return Optional.empty();
  }

  static SqrlHint of(@NonNull final String name) {
    return new SqrlHint() {
      @Override
      public RelHint getHint() {
        return RelHint.builder(name).build();
      }

      @Override
      public String getHintName() {
        return name;
      }
    };
  }

  public interface Constructor<H extends SqrlHint> {

    boolean validName(String name);

    H fromHint(RelHint hint);

  }

}
