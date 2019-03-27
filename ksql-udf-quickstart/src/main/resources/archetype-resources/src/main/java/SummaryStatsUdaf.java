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

package ${package};

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.Map;
import java.util.HashMap;

/**
 * In this example, we implement a UDAF for computing some summary statistics for a stream
 * of doubles.
 *
 * Example query usage:
 *
 * CREATE STREAM api_responses (username VARCHAR, response_code INT, response_time DOUBLE) \
 * WITH (kafka_topic='api_logs', value_format='JSON');
 *
 * SELECT username, SUMMARY_STATS(response_time) \
 * FROM api_responses \
 * GROUP BY username ;
 */
@UdafDescription(
  name = "summary_stats",
  description = "Example UDAF that computes some summary stats for a stream of doubles"
)
public class SummaryStatsUdaf {

  @UdafFactory(description = "compute summary stats for doubles")
  // Can be used with stream aggregations. The input of our aggregation will be doubles,
  // and the output will be a map
  public static Udaf<Double, Map<String, Double>> createUdaf() {

    return new Udaf<Double, Map<String, Double>>() {

      // Specify an initial value for our aggregation
      @Override
      public Map<String, Double> initialize() {
        final Map<String, Double> stats = new HashMap<>();
        stats.put("mean", 0.0);
        stats.put("sample_size", 0.0);
        stats.put("sum", 0.0);
        return stats;
      }

      // Perform the aggregation whenever a new record appears in our stream
      @Override
      public Map<String, Double> aggregate(final Double currentValue, final Map<String,Double> aggregateValue) {
        final Double sampleSize = aggregateValue.getOrDefault("sample_size", 0.0) + 1.0;
        final Double sum =  aggregateValue.getOrDefault("sum", 0.0) + currentValue;
        // calculate the new aggregate
        Map<String, Double> newAggregate = new HashMap<>();
        newAggregate.put("mean", sum / sampleSize);
        newAggregate.put("sample_size", sampleSize);
        newAggregate.put("sum", sum);
        return newAggregate;
      }

      // Specify what should happen when aggregations are merged. This method
      // should produce a combined aggregate
      @Override
      public Map<String,Double> merge(final Map<String, Double> aggOne, final Map<String, Double> aggTwo) {
        final Double sampleSize =
            aggOne.getOrDefault("sample_size", 0.0) + aggTwo.getOrDefault("sample_size", 0.0);
        final Double sum =
            aggOne.getOrDefault("sum", 0.0) + aggTwo.getOrDefault("sum", 0.0);

        // calculate the new aggregate
        Map<String, Double> newAggregate = new HashMap<>();
        newAggregate.put("mean", sum / sampleSize);
        newAggregate.put("sample_size", sampleSize);
        newAggregate.put("sum", sum);
        return newAggregate;
      }
    };
  }
}
