/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.TypeUtil;
import org.apache.kafka.connect.data.Schema;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateFunction
    extends Statement implements ExecutableDdlStatement {
  private final QualifiedName name;
  private final List<TableElement> elements;
  private final String language;
  private final String script;
  private final Schema returnType;

  public CreateFunction(
      final QualifiedName name,
      final List<TableElement> elements,
      final String language,
      final String script,
      final Schema returnType) {
    this(Optional.empty(), name, elements, language, script, returnType);
  }

  public CreateFunction(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final List<TableElement> elements,
      final String language,
      final String script,
      final Schema returnType) {
    super(location);
    this.name = requireNonNull(name, "function name is null");
    this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
    this.language = formatLanguage(requireNonNull(language, "language name is null"));
    this.script = requireNonNull(script, "ud(a)f script is null");
    this.returnType = requireNonNull(returnType, "reutrn type is null");
  }

  public boolean isExecutable() {
    try (Context context = Context.create(getLanguage())) {
      Value function = context.eval(getLanguage(), getScript());
      return function.canExecute();
    } catch (Exception e) {
      // TODO: make sure the exception bubbles up somewhere
      return false;
    }
  }

  public String formatLanguage(String lang) {
    lang = lang.toLowerCase().trim();
    if (lang.equals("javascript")) {
      lang = "js";
    }
    return lang;
  }

  public String getName() {
    return name.toString();
  }

  public List<Schema> getArguments() {
    List<Schema> arguments = new ArrayList<>();
    for (TableElement element : elements) {
      arguments.add(TypeUtil.getTypeSchema(element.getType()));
    }
    return arguments;
  }

  public String getLanguage() {
    return language;
  }

  public Schema getReturnType() {
    return returnType;
  }

  public String getScript() {
    return script;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateFunction(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, elements);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateFunction o = (CreateFunction) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(elements, o.elements)
           && Objects.equals(language, o.language)
           && Objects.equals(script, o.script);
  }

  @Override
  public String toString() {
    return getName();
  }
}
