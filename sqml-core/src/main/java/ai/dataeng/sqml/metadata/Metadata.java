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
package ai.dataeng.sqml.metadata;

import ai.dataeng.sqml.StubModel.ModelRelation;
import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.common.CatalogSchemaName;
import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.common.type.TypeSignature;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.relation.TableHandle;
import java.sql.Connection;
import java.util.Map;
import java.util.Optional;

public interface Metadata {

  Type getType(TypeSignature signature);

  boolean schemaExists(Session session, CatalogSchemaName schema);

  Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

  @Deprecated
  boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle);

  TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

  Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);

  Optional<Object> getCatalogHandle(Session session, String catalogName);

  FunctionAndTypeManager getFunctionAndTypeManager();

  Connection getConnection(Session session);

  Optional<TableHandle> getSourceTable(Source source);

  void notifyColumn(String translatedName, String name, String type);

  void createSourceTable(Source source);

  void createView(ModelRelation rel);

  Optional<TableHandle> getRelationHandle(Session session, QualifiedObjectName name);

  ColumnHandle getColumnHandle(Session session, String name);
}
