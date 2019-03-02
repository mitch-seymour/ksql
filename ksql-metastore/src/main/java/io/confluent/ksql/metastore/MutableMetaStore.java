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

package io.confluent.ksql.metastore;

import java.util.Set;

import io.confluent.ksql.function.MutableFunctionRegistry;

public interface MutableMetaStore extends MetaStore {

  MutableFunctionRegistry getFunctionRegistry();

  void putTopic(KsqlTopic topic);

  void putSource(StructuredDataSource dataSource);

  void deleteTopic(String topicName);

  void deleteSource(String sourceName);

  void updateForPersistentQuery(
      String queryId,
      Set<String> sourceNames,
      Set<String> sinkNames);

  void removePersistentQuery(String queryId);

  MutableMetaStore copy();
}
