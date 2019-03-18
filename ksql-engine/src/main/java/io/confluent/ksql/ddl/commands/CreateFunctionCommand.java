/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.ddl.commands;

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

import java.lang.reflect.Constructor;
import java.util.function.Function;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.language;

public class CreateFunctionCommand implements DdlCommand {

  private static final Logger LOG = LoggerFactory.getLogger(CreateFunctionCommand.class);

  private final CreateFunction createFunction;

  CreateFunctionCommand(
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
  static Class<? extends Kudf> getKudf(final String language) {
    abstract class CustomKudf implements Kudf {
      protected final String[] argumentNames;
      protected final String language;
      protected final String name;
      protected final Class returnType;
      protected final String script;

      public CustomKudf(final CreateFunction cf) {
        this.argumentNames = cf.getArgumentNames();
        this.language = cf.getLanguage();
        this.name = cf.getName();
        this.returnType = SchemaUtil.getJavaType(cf.getReturnType());
        this.script = cf.getScript();
      }

      @Override
      public String toString() {
        return name;
      }
    }

    class InlineMultilingualUdf extends CustomKudf {
      private final Context context;

      public InlineMultilingualUdf(final CreateFunction cf) {
        super(cf);
        // build the polyglot context.
        // TODO: set the security settings from new KSQL configs
        this.context = Context.newBuilder(new String[]{language})
          .allowIO(true)
          .allowHostAccess(true)
          .allowNativeAccess(true)
          .build();
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        Object value;
        try {
          final Value bindings = context.getPolyglotBindings();
          for (int i = 0; i < args.length; i++) {
            // TODO: argument names are converted to all caps. either make sure this is
            // documented, or figure out how to maintain original case
            bindings.putMember(argumentNames[i], args[i]);
          }
          value = context.eval(language, script).as(returnType);
        } catch (Exception e) {
          LOG.warn("Exception encountered while executing function. Setting value to null", e);
          value = null;
        }
        return value;
      }
    }

    class InlineJavaUdf extends CustomKudf {
      private final IScriptEvaluator se;

      public InlineJavaUdf(final CreateFunction cf) throws Exception {
        super(cf);
        se = CompilerFactoryFactory
            .getDefaultCompilerFactory()
            .newScriptEvaluator();
        se.setReturnType(returnType);
        se.setParameters(argumentNames, new Class[] { String.class, String.class });
        se.cook(script);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Object evaluate(final Object... args) {
        Object value;
        try {
          value = se.evaluate(args);
        } catch (Exception e) {
          LOG.warn("Exception encountered while executing function. Setting value to null", e);
          value = null;
        }
        return value;
      }
    }

    if (language.trim().equalsIgnoreCase("java")) {
      return InlineJavaUdf.class;
    }

    return InlineMultilingualUdf.class;
  }

  Function<KsqlConfig, Kudf> getUdfFactory(final Class<? extends Kudf> kudfClass) {
    return ksqlConfig -> {
      try {
        final Constructor<? extends Kudf> constructor =
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

      final Class<? extends Kudf> kudfClass = getKudf(createFunction.getLanguage());
      final Function<KsqlConfig, Kudf> udfFactory = getUdfFactory(kudfClass);

      final KsqlFunction ksqlFunction = KsqlFunction.create(
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
