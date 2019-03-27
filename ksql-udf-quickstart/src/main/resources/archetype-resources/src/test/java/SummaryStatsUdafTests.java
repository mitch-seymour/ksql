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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.confluent.ksql.function.udaf.Udaf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Example class that demonstrates how to unit test UDAFs.
 */
public class SummaryStatsUdafTests {

  @ParameterizedTest
  @MethodSource("aggSources")
  void calculateSummaryStats(Double newValue, Map<String, Double> currentAggregate, Map<String, Double> expectedResult) {
    final Udaf<Double, Map<String, Double>> udaf = SummaryStatsUdaf.createStddev();
    assertEquals(expectedResult, udaf.aggregate(newValue, currentAggregate));
  }

  static Stream<Arguments> aggSources() {
    return Stream.of(
      // sample: 400
      arguments(
        // new value
        400.0,
        // current aggregate
        result(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        // expected new aggregate
        result(400.0, 1.0, 400.0, 160000.0, 0.0, 0.0)
      ),
      // sample: 400, 900
      arguments(
        // new value
        900.0,
        // current aggregate
        result(400.0, 1.0, 400.0, 160000.0, 0.0, 0.0),
        // expected new aggregate
        result(650.0, 2.0, 1300.0, 970000.0, 250.0, 353.5533905932738)
      ),
      // sample: 400, 900, 800
      arguments(
        // new value
        800.0,
        // current aggregate
        result(650.0, 2.0, 1300.0, 970000.0, 250.0, 353.5533905932738),
        // expected new aggregate
        result(700.0, 3.0, 2100.0, 1610000.0, 216.0246899469286, 264.57513110645897)
      )
    );
  }

  static Map<String, Double> result(
    Double mean,
    Double sampleSize,
    Double sum,
    Double sumSquares,
    Double stddevPopulation,
    Double stddevSample) {

    Map<String, Double> result = new HashMap<>();
    result.put("mean", mean);
    result.put("sample_size", sampleSize);
    result.put("sum", sum);
    result.put("sum_squares", sumSquares);
    result.put("stddev_population", stddevPopulation);
    result.put("stddev_sample", stddevSample);
    return result;
  }
}

