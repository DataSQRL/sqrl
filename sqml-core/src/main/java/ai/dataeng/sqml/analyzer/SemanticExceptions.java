/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Node;
import ai.dataeng.sqml.sql.tree.NodeLocation;
import ai.dataeng.sqml.sql.tree.QualifiedName;

public final class SemanticExceptions {

  private SemanticExceptions() {
  }

  public static SemanticException missingAttributeException(Expression node, QualifiedName name) {
    throw new SemanticException(
        MISSING_ATTRIBUTE,
        node,
        name.getPrefix().isPresent() ? "'%s' cannot be resolved" : "Column '%s' cannot be resolved",
        name);
  }

  /**
   * Use {@link #missingAttributeException(Expression, QualifiedName)} instead.
   */
  @Deprecated
  public static SemanticException missingAttributeException(Expression node) {
    throw new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", node);
  }

  public static SemanticException ambiguousAttributeException(Expression node, QualifiedName name) {
    throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", name);
  }

  public static SemanticException notSupportedException(Node node,
      String notSupportedFeatureDescription) {
    throw new SemanticException(NOT_SUPPORTED, node,
        notSupportedFeatureDescription + " is not supported");
  }

  public static String subQueryNotSupportedError(Node node, String notSupportedFeatureDescription) {
    if (node.getLocation().isPresent()) {
      NodeLocation nodeLocation = node.getLocation().get();
      return format("line %s:%s: %s", nodeLocation.getLineNumber(), nodeLocation.getColumnNumber(),
          notSupportedFeatureDescription + " is not supported");
    }
    return notSupportedFeatureDescription + " is not supported";
  }
}
