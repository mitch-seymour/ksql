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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateFunction extends Statement {
  private final QualifiedName name;
  private final List<TableElement> elements;

  public CreateFunction(
      final QualifiedName name,
      final List<TableElement> elements) {
    this(Optional.empty(), name, elements);
  }

  public CreateFunction(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final List<TableElement> elements) {
    super(location);
    System.out.println("CreateFunction::constructor");
    this.name = requireNonNull(name, "function name is null");
    this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
  }

  public QualifiedName getName() {
    return name;
  }

  public List<TableElement> getElements() {
    return elements;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    System.out.println("CreateFunction::accept");
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
           && Objects.equals(elements, o.elements);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("elements", elements)
        .toString();
  }
}
