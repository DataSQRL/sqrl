/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import java.util.List;

import org.apache.commons.collections.ListUtils;

import com.datasqrl.plan.global.MaterializationPreference;
import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class MaterializationInference {

  public static final String DEFAULT_REASON = "default";

  final MaterializationPreference preference;
  final List<String> reasons;

  public MaterializationInference(MaterializationPreference defaultPreference) {
    this(defaultPreference, DEFAULT_REASON);
  }

  public MaterializationInference(MaterializationPreference defaultPreference, String reason) {
    this(defaultPreference, List.of(reason));
  }

  MaterializationInference update(MaterializationPreference preference, String reason) {
    var other = new MaterializationInference(preference,
        ListUtils.union(this.reasons, List.of(reason)));
    Preconditions.checkArgument(this.preference.isCompatible(preference),
        "Incompatible materialization preferences: [%s] vs [%s]",
        this, other);
    return other;
  }

  MaterializationInference combine(MaterializationInference other) {
    Preconditions.checkArgument(this.preference.isCompatible(other.preference),
        "Incompatible materialization preferences: [%s] vs [%s]",
        this, other);
    return new MaterializationInference(this.preference.combine(other.preference),
        ListUtils.union(this.reasons, other.reasons));
  }

  @Override
  public String toString() {
    return preference + " because " + reasons;
  }

}
