/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
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

        DataStream<ClickEvent> clicksWithTs = clicks
              .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                          .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                          .withTimestampAssigner((evt, ts) -> evt.timestamp())
              );


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
                      String productName = count > 0 ? elements.iterator().next().productName() : "";
                      double currentRate = count / 5.0; // clicks per minute

                      out.collect(new DemandMetrics(productId, productName, currentRate, 0.0));
                  }
              });

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
                      String productName = count > 0 ? elements.iterator().next().productName() : "";
                      double avgRate = count / 60.0;

                      out.collect(new DemandMetrics(productId, productName, 0.0, avgRate));
                  }
              });

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
                            curr.productName(),
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
