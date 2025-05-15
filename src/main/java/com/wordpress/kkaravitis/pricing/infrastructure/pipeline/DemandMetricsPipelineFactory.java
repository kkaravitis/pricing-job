package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import java.time.Duration;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class DemandMetricsPipelineFactory {

    public void build(DataStream<ClickEvent> clicks) {
        // 1) Add timestamps & watermarks
        DataStream<ClickEvent> clicksWithTs = clicks
              .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                          .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                          .withTimestampAssigner((evt, ts) -> evt.getTimestamp())
              );

        // 2) Compute current demand (5‑min sliding window, slide 1 min)
        SingleOutputStreamOperator<DemandMetrics> shortWindow = clicksWithTs
              .keyBy(ClickEvent::getProductId)
              .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
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
              .keyBy(ClickEvent::getProductId)
              .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
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
              .keyBy(DemandMetrics::getProductId)
              .intervalJoin(longWindow.keyBy(DemandMetrics::getProductId))
              .between(Time.seconds(-150), Time.seconds(150))  // ±2.5 min
              .process(new ProcessJoinFunction<>() {
                  @Override
                  public void processElement(
                        DemandMetrics curr,
                        DemandMetrics hist,
                        Context ctx,
                        Collector<DemandMetrics> out) {
                      out.collect(new DemandMetrics(
                            curr.getProductId(),
                            curr.getCurrentDemand(),
                            hist.getHistoricalAverage()
                      ));
                  }
              });

        FlinkDemandMetricsRepository demandMetricsRepository = new FlinkDemandMetricsRepository();
        demandMetricsStream
              .keyBy(DemandMetrics::getProductId)
              .process(new KeyedProcessFunction<String, DemandMetrics, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      demandMetricsRepository.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(DemandMetrics dm, Context ctx, Collector<Void> out)
                        throws Exception {
                      demandMetricsRepository.updateMetrics(dm);
                  }
              })
              .name("UpdateDemandMetricsState");
    }
}
