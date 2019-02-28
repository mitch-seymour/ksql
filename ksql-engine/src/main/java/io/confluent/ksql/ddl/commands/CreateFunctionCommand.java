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

import static com.google.common.base.MoreObjects.toStringHelper;

import java.lang.reflect.Constructor;
import java.util.function.Function;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

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
import io.confluent.ksql.util.SchemaUtil;
import scala.language;

public class CreateFunctionCommand implements DdlCommand {
  private final CreateFunction createFunction;

  CreateFunctionCommand(
      final String sqlExpression,
      final CreateFunction createFunction
  ) {

    if (!createFunction.isExecutable()) {
      // TODO: improve the error text here
      throw new KsqlException(
          "Function is not executable"
      );
    }

    this.createFunction = createFunction;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", createFunction.getName())
        .add("language", createFunction.getLanguage())
        .toString();
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
        return toStringHelper(this)
            .add("name", name)
            .add("language", language)
            .toString();
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        System.out.println("Running script 1");
        Object value;
        try {
          value = function.execute(args).as(returnType);
          System.out.println("Running script 3");
        } catch (Exception e) {
          System.out.println("Error executing function: " + toString() + ", " + e.getMessage());
          e.printStackTrace();
          value = null;
        }
        return value;
      }
    }

    return CustomKudf.class;
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {

    // this is ugly af. just poc'ing
    // TODO: wrap in a try-catch
    if (metaStore instanceof MetaStoreImpl) {
      final MetaStoreImpl m = (MetaStoreImpl) metaStore;
      if (m.functionRegistry instanceof MutableFunctionRegistry) {
        final MutableFunctionRegistry f = (MutableFunctionRegistry) m.functionRegistry;

        // create an custom Kudf class that implements the script
        // note: anonymous classes don't work here because they lack a constructor,
        // and the codegen throws a missing <init> method
        Class<? extends Kudf> kudfClass = getKudf();

        String functionName = createFunction.getName();

        final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
          try {
            Constructor<? extends Kudf> constructor = 
                kudfClass.getConstructor(CreateFunction.class);
            return constructor.newInstance(createFunction);
          } catch (Exception e) {
            throw new KsqlException("Failed to create instance of kudfClass "
            + kudfClass + " for function " + createFunction.getName(), e);
          }
        };

        String description = "Scripted UD(A)F: " + toString();
        KsqlFunction ksqlFunction = KsqlFunction.create(
            createFunction.getReturnType(),
            createFunction.getArguments(),
            functionName,
            kudfClass,
            udfFactory,
            description,
            KsqlFunction.INTERNAL_PATH);
        
        final UdfMetadata metadata = new UdfMetadata(ksqlFunction.getFunctionName(),
            description,
            "Mitch Seymour",
            "0.1.0",
            KsqlFunction.INTERNAL_PATH,
            true);

        // TODO: figure out why this code path is called twice remove this debug text. 
        System.out.println("Creating function from thread: " + Thread.currentThread().getName());
        f.ensureFunctionFactory(new UdfFactory(ksqlFunction.getKudfClass(), metadata));
        f.addFunction(ksqlFunction);
      }
      return new DdlCommandResult(true, "Function created");
    }
    // TODO: make this more informative
    return new DdlCommandResult(true, "Could not create function");
  }
}
