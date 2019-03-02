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

import java.lang.reflect.Constructor;
import java.util.function.Function;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import scala.language;

public class CreateFunctionCommand implements DdlCommand {

  private static final Logger LOG = LoggerFactory.getLogger(CreateFunctionCommand.class);

  private final CreateFunction createFunction;

  CreateFunctionCommand(
      final String sqlExpression,
      final CreateFunction createFunction
  ) {

    this.createFunction = createFunction;
  }

  @Override
  public String toString() {
    return createFunction.getName();
  }

  /**
   * A method for retrieving a custom Kudf class. This class gets instantiated
   * everytime a query is created with the custom function.
   */
  public static Class<? extends Kudf> getKudf() {
    class CustomKudf implements Kudf {
      private final Context context;
      private final Value function;
      private final String language;
      private final String name;
      private final Class returnType;

      public CustomKudf(CreateFunction cf) {
        this.language = cf.getLanguage();
        this.name = cf.getName();
        this.context = Context.create(language);
        this.function = context.eval(language, cf.getScript());
        this.returnType = SchemaUtil.getJavaType(cf.getReturnType());
      }

      @Override
      public String toString() {
        return name;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        Object value;
        try {
          value = function.execute(args).as(returnType);
        } catch (Exception e) {
          LOG.warn("Exception encountered while executing function. Setting value to null", e);
          value = null;
        }
        return value;
      }
    }

    return CustomKudf.class;
  }

  Function<KsqlConfig, Kudf> getUdfFactory(Class<? extends Kudf> kudfClass) {
    return ksqlConfig -> {
      try {
        Constructor<? extends Kudf> constructor =
            kudfClass.getConstructor(CreateFunction.class);
        return constructor.newInstance(createFunction);
      } catch (Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
        + kudfClass + " for function " + createFunction.getName(), e);
      }
    };
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    try {
      final MutableFunctionRegistry functionRegistry = metaStore.getFunctionRegistry();

      final Class<? extends Kudf> kudfClass = getKudf();
      final Function<KsqlConfig, Kudf> udfFactory = getUdfFactory(kudfClass);

      KsqlFunction ksqlFunction = KsqlFunction.create(
              createFunction.getReturnType(),
              createFunction.getArguments(),
              createFunction.getName(),
              kudfClass,
              udfFactory,
              createFunction.getDescription(),
              KsqlFunction.INTERNAL_PATH);

      final UdfMetadata metadata = new UdfMetadata(
              createFunction.getName(),
              createFunction.getOverview(),
              createFunction.getAuthor(),
              createFunction.getVersion(),
              KsqlFunction.INTERNAL_PATH,
              false);

      functionRegistry.ensureFunctionFactory(new UdfFactory(ksqlFunction.getKudfClass(), metadata));

      if (createFunction.shouldReplace()) {
        functionRegistry.addOrReplaceFunction(ksqlFunction);
      } else {
        functionRegistry.addFunction(ksqlFunction);
      }

      return new DdlCommandResult(true, "Function created");

    } catch (Exception e) {
      final String errorMessage =
            String.format("Cannot create function '%s': %s",
                createFunction.getName(), e.getMessage());
      throw new KsqlException(errorMessage, e);
    }
  }
}
