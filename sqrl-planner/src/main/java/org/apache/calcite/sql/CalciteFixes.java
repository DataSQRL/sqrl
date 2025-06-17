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
package org.apache.calcite.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class CalciteFixes {

  public static List<SqlParserPos> getComponentPositions(SqlIdentifier identifier) {
    return identifier.componentPositions == null
        ? List.of()
        : new ArrayList<>(identifier.componentPositions);
  }

  public static void appendSelectLists(SqlNode node) {
    // calcite quirk
    node.accept(
        new SqlShuttle() {
          @Override
          public SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.SELECT) {
              var select1 = ((SqlSelect) call);

              if (select1.getSelectList() == null) {
                select1.setSelectList(
                    new SqlNodeList(List.of(SqlIdentifier.STAR), SqlParserPos.ZERO));
              }
            }

            return super.visit(call);
          }
        });
  }

  // odd behavior by calcite parser, im doing something wrong?
  public static SqlNode pushDownOrder(SqlNode sqlNode) {
    // recursive?
    if (sqlNode instanceof SqlOrderBy order && ((SqlOrderBy) sqlNode).query instanceof SqlSelect) {
      var select = ((SqlSelect) order.query);
      select.setOrderBy(order.orderList);
      select.setFetch(order.fetch);
      select.setOffset(order.offset);
      sqlNode = select;
    }
    return sqlNode;
  }
}
