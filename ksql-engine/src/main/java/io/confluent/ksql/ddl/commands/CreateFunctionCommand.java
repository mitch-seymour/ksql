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

import java.util.Collections;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public class CreateFunctionCommand implements DdlCommand {
  private final CreateFunction createFunction;

  CreateFunctionCommand(
      final String sqlExpression,
      final CreateFunction createFunction
  ) {

    if (1 == 2) {
      throw new KsqlException(
          "Cannot define a TABLE without providing the KEY column name in the WITH clause."
      );
    }

    this.createFunction = createFunction;
  }

  public static Class<? extends Kudf> getKudf() {
    class CustomKudf implements Kudf {
      public CustomKudf() {
        System.out.println("Instantiating CustomKudf");
      }

      @Override
      public Object evaluate(final Object... args) {
        return "hello,";
      }
    }

    return CustomKudf.class;
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {

    // this is ugly af. just poc'ing
    if (metaStore instanceof MetaStoreImpl) {
      final MetaStoreImpl m = (MetaStoreImpl) metaStore;
      if (m.functionRegistry instanceof MutableFunctionRegistry) {
        final MutableFunctionRegistry f = (MutableFunctionRegistry) m.functionRegistry;

        // create an custom Kudf class that implements the script
        // note: anonymous classes don't work here because they lack a constructor,
        // and the codegen throws a missing <init> method
        Class<? extends Kudf> kudfClass = getKudf();

        String functionName = createFunction.getName().toString();

        final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
          try {
            return kudfClass.newInstance();
          } catch (Exception e) {
            throw new KsqlException("Failed to create instance of kudfClass "
            + kudfClass + " for function " + createFunction.getName(), e);
          }
        };

        KsqlFunction ksqlFunction = KsqlFunction.create(
            Schema.STRING_SCHEMA,
            Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
            functionName,
            kudfClass,
            udfFactory,
            "hello, world",
            KsqlFunction.INTERNAL_PATH);
        
        final UdfMetadata metadata = new UdfMetadata(ksqlFunction.getFunctionName(),
            "description of my function",
            "Mitch Seymour",
            "0.1.0",
            KsqlFunction.INTERNAL_PATH,
            true);

        
        f.ensureFunctionFactory(new UdfFactory(ksqlFunction.getKudfClass(), metadata));
        f.addFunction(ksqlFunction);
      }
    }
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
    System.out.println("listing functions");
    System.out.println(metaStore.listFunctions());
    return new DdlCommandResult(true, "Function created");
  }
}
