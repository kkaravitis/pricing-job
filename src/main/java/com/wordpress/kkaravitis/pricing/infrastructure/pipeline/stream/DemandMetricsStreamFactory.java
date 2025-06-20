/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import java.time.Duration;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;

public class DemandMetricsStreamFactory {

    public DataStream<MetricUpdate> build(DataStream<ClickEvent> clicks) {
        // 1) Add timestamps & watermarks
        DataStream<ClickEvent> clicksWithTs = clicks
              .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                          .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                          .withTimestampAssigner((evt, ts) -> evt.timestamp())
              );

        // 2) Compute current demand (5‑min sliding window, slide 1 min)
        SingleOutputStreamOperator<DemandMetrics> shortWindow = clicksWithTs
              .keyBy(ClickEvent::productId)
              .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofMinutes(1)))
              .process(new ProcessWindowFunction<>() {
                  @Override
                  public void process(
                        String productId,
                        Context ctx,
                        Iterable<ClickEvent> elements,
                        Collector<DemandMetrics> out) {

                      long count = StreamSupport.stream(elements.spliterator(), false).count();
                      double currentRate = count / 5.0; // clicks per minute

                      // emit with placeholder historical—will be filled later
                      out.collect(new DemandMetrics(productId, currentRate, 0.0));
                  }
              });

        // 3) Compute historical average (1‑hour sliding window, slide 5 min)
        SingleOutputStreamOperator<DemandMetrics> longWindow = clicksWithTs
              .keyBy(ClickEvent::productId)
              .window(SlidingEventTimeWindows.of(Duration.ofHours(1), Duration.ofMinutes(5)))
              .process(new ProcessWindowFunction<>() {
                  @Override
                  public void process(
                        String productId,
                        Context ctx,
                        Iterable<ClickEvent> elements,
                        Collector<DemandMetrics> out) {
                      long count = StreamSupport.stream(elements.spliterator(), false).count();
                      double avgRate = count / 60.0; // clicks per minute over last hour
                      // emit with placeholder current—will be filled later
                      out.collect(new DemandMetrics(productId, 0.0, avgRate));
                  }
              });

        // 4) Join the two streams by productId and window alignment
        //    We’ll use a simple interval join on event time, matching each shortWindow record
        //    with the latest longWindow record within ±2.5 minutes of its window end.

        DataStream<DemandMetrics> demandMetricsStream = shortWindow
              .keyBy(DemandMetrics::productId)
              .intervalJoin(longWindow.keyBy(DemandMetrics::productId))
              .between(Duration.ofSeconds(-150), Duration.ofSeconds(150))  // ±2.5 min
              .process(new ProcessJoinFunction<>() {
                  @Override
                  public void processElement(
                        DemandMetrics curr,
                        DemandMetrics hist,
                        Context ctx,
                        Collector<DemandMetrics> out) {
                      out.collect(new DemandMetrics(
                            curr.productId(),
                            curr.currentDemand(),
                            hist.historicalAverage()
                      ));
                  }
              });

        return demandMetricsStream.map(dm -> new MetricUpdate(
              dm.productId(),
              MetricType.DEMAND,
              dm
        ));
    }
}
