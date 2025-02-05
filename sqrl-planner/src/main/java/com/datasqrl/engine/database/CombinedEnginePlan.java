package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class CombinedEnginePlan implements EnginePhysicalPlan {

  @Singular
  Map<String, EnginePhysicalPlan> plans;

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    List<DeploymentArtifact> combined = new ArrayList<>();
    plans.forEach((name,plan) -> combined.addAll(
        plan.getDeploymentArtifacts().stream().map(artifact -> new DeploymentArtifact(
            name.isBlank()? artifact.getFileSuffix() : ("-" + name + artifact.getFileSuffix()),
            artifact.getContent())).collect(
            Collectors.toList())));
    return combined;
  }
}
