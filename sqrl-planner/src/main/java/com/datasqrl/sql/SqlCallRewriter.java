/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.sql;

import com.datasqrl.canonicalizer.NamePath;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

/** Extracts modules and rewrites calls */
public class SqlCallRewriter extends SqlShuttle {
  Set<NamePath> fncModules = new LinkedHashSet<>();

  public void performCallRewrite(SqlCall sqlNode) {
    sqlNode.accept(this);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call.getOperator().getNameAsId().names.size() > 1) {
      var nameAsId = call.getOperator().getNameAsId();
      var array = nameAsId.names.toArray(String[]::new);
      var names = NamePath.of(array);
      fncModules.add(names);

      var component = nameAsId.getComponent(nameAsId.names.size() - 1);
      nameAsId.setNames(
          List.of(component.names.get(0)), List.of(component.getComponentParserPosition(0)));
    }

    return super.visit(call);
  }

  public Set<NamePath> getFncModules() {
    return fncModules;
  }
}
