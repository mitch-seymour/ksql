/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.util.KsqlException;

public class CreateFunctionCommand implements DdlCommand {

  CreateFunctionCommand(
      final String sqlExpression,
      final CreateFunction createFunction
  ) {

    if (1 == 2) {
      throw new KsqlException(
          "Cannot define a TABLE without providing the KEY column name in the WITH clause."
      );
    }
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    /*
    final KsqlTable ksqlTable = new KsqlTable<>(
        sqlExpression,
        sourceName,
        schema,
        (keyColumnName.isEmpty())
          ? null : SchemaUtil.getFieldByName(schema, keyColumnName).orElse(null),
        timestampExtractionPolicy,
        metaStore.getTopic(topicName),
        stateStoreName, keySerde
    );

    metaStore.putSource(ksqlTable.cloneWithTimeKeyColumns());
    */
    return new DdlCommandResult(true, "Function create");
  }
}
