package com.mycompany.pricing.infrastructure;

import com.mycompany.pricing.adapters.competitor.HttpCompetitorPriceProvider;
import com.mycompany.pricing.adapters.competitor.OkHttpServiceClient;
import com.mycompany.pricing.domain.model.ClickEvent;
import com.mycompany.pricing.domain.model.CompetitorPrice;
import com.mycompany.pricing.domain.model.DemandMetrics;
import com.mycompany.pricing.domain.model.InventoryEvent;
import com.mycompany.pricing.domain.model.Money;
import com.mycompany.pricing.domain.model.PriceRule;
import com.mycompany.pricing.domain.model.PriceRuleUpdate;
import com.mycompany.pricing.infrastructure.async.FlinkAsyncCompetitorEnrichment;
import com.mycompany.pricing.infrastructure.process.PricingWithModelBroadcastFunction;
import com.mycompany.pricing.infrastructure.provider.BroadcastModelInferencePort;
import com.mycompany.pricing.infrastructure.provider.FlinkCompetitorPriceProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkDemandMetricsProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkInventoryProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkPriceRuleProvider;
import com.mycompany.pricing.infrastructure.source.InventoryCdcSource;
import com.mycompany.pricing.infrastructure.source.KafkaClickEventSource;
import com.mycompany.pricing.infrastructure.source.KafkaModelBroadcastSource;
import com.mycompany.pricing.infrastructure.source.PriceRuleCdcSource;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Flink job that unifies price-rule and model updates via a single broadcast stream, enriches with async competitor prices, and computes dynamic pricing.
 */
public class FlinkDynamicPricingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Click events
        KafkaClickEventSource clicksSource =
              new KafkaClickEventSource("localhost:9092", "click-topic", "click-group");
        DataStream<ClickEvent> clicks = clicksSource.create(env);

        // collect demand metrics per product.
        FlinkDemandMetricsProvider demandMetricsProvider = feedFlinkDemandMetricsProvider(clicks);
        // collect inventory level per product.
        FlinkInventoryProvider inventoryProvider = feedFlinkInventoryProvider(env);
        // collect competitors price per product.
        FlinkCompetitorPriceProvider priceProvider = feedFlinkCompetitorPriceProvider(clicks);
        // collect product price rule.
        FlinkPriceRuleProvider ruleProvider = feedFlinkPriceRuleProvider(env);

        KafkaModelBroadcastSource modelCdc =
              new KafkaModelBroadcastSource("localhost:9092", "model-topic", "model-group");

        BroadcastModelInferencePort modelPort =
              new BroadcastModelInferencePort("current-model");

        // 7) Unified broadcast-process for pricing
        clicks
              .keyBy(ClickEvent::getProductId)
              .connect(modelCdc.create(env))
              .process(new PricingWithModelBroadcastFunction(
                    ruleProvider,
                    demandMetricsProvider,
                    inventoryProvider,
                    priceProvider,
                    modelPort
              ))
              .name("DynamicPricingUnified")
              .print();

        env.execute("Flink Dynamic Pricing Job");
    }

    private static FlinkDemandMetricsProvider feedFlinkDemandMetricsProvider(DataStream<ClickEvent> clicks) {
        // 1) Add timestamps & watermarks
        DataStream<ClickEvent> clicksWithTs = clicks
              .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                          .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                          .withTimestampAssigner((evt, ts) -> evt.getTimestamp())
              );

        // 2) Compute current demand (5‑min sliding window, slide 1 min)
        SingleOutputStreamOperator<DemandMetrics> shortWindow = clicksWithTs
              .keyBy(ClickEvent::getProductId)
              .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
              .process(new ProcessWindowFunction<ClickEvent, DemandMetrics, String, TimeWindow>() {
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

        // 3) Compute historical average (1‑hour sliding window, slide 5 min)
        SingleOutputStreamOperator<DemandMetrics> longWindow = clicksWithTs
              .keyBy(ClickEvent::getProductId)
              .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
              .process(new ProcessWindowFunction<ClickEvent, DemandMetrics, String, TimeWindow>() {
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
        //    with the latest longWindow record within ±2.5 minutes of its window end.

        DataStream<DemandMetrics> demandStream = shortWindow
              .keyBy(DemandMetrics::getProductId)
              .intervalJoin(longWindow.keyBy(DemandMetrics::getProductId))
              .between(Time.seconds(-150), Time.seconds(150))  // ±2.5 min
              .process(new ProcessJoinFunction<DemandMetrics, DemandMetrics, DemandMetrics>() {
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

        FlinkDemandMetricsProvider demandProv = new FlinkDemandMetricsProvider();
        demandStream
              .keyBy(DemandMetrics::getProductId)
              .process(new KeyedProcessFunction<String, DemandMetrics, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      demandProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(DemandMetrics dm, Context ctx, Collector<Void> out)
                        throws Exception {
                      demandProv.updateMetrics(dm.getProductId(), dm);
                  }
              })
              .name("UpdateDemandMetricsState");

        return demandProv;
    }

    private static FlinkInventoryProvider feedFlinkInventoryProvider(StreamExecutionEnvironment env) {
        InventoryCdcSource invCdc = new InventoryCdcSource(
              "db-host", 3306,
              "warehouse_db", "inventory",
              "dbuser", "dbpass"
        );
        DataStream<InventoryEvent> inventoryStream = invCdc.create(env);
        FlinkInventoryProvider invProv = new FlinkInventoryProvider();
        inventoryStream
              .keyBy(InventoryEvent::getProductId)
              .process(new KeyedProcessFunction<String, InventoryEvent, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      invProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(InventoryEvent ie, Context ctx, Collector<Void> out)
                        throws Exception {
                      invProv.updateLevel(ie.getLevel());
                  }
              })
              .name("UpdateInventoryState");
        return invProv;
    }

    private static FlinkCompetitorPriceProvider feedFlinkCompetitorPriceProvider(DataStream<ClickEvent> clicks) {
        // Async competitor price lookup
        HttpCompetitorPriceProvider httpProv =
              new HttpCompetitorPriceProvider(new OkHttpServiceClient(), "http://api.example.com");
        FlinkAsyncCompetitorEnrichment asyncEnrich = new FlinkAsyncCompetitorEnrichment(httpProv);
        SingleOutputStreamOperator<String> prodIds = clicks
              .map(ClickEvent::getProductId).name("ExtractProductId");
        DataStream<CompetitorPrice> compPrices = AsyncDataStream
              .unorderedWait(prodIds, asyncEnrich, 2000, TimeUnit.MILLISECONDS, 50)
              .name("AsyncCompetitorEnrichment");
        FlinkCompetitorPriceProvider priceProv = new FlinkCompetitorPriceProvider();
        compPrices
              .keyBy(CompetitorPrice::getProductId)
              .process(new KeyedProcessFunction<String, CompetitorPrice, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      priceProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(CompetitorPrice cp, Context ctx, Collector<Void> out)
                        throws Exception {
                      priceProv.updatePrice(cp);
                  }
              }).name("UpdateCompetitorState");

        return priceProv;
    }

    private static FlinkPriceRuleProvider feedFlinkPriceRuleProvider(StreamExecutionEnvironment env) {
        PriceRuleCdcSource ruleCdc = new PriceRuleCdcSource(
              "db-host", 3306, "pricing_db", "price_rules", "dbuser", "dbpass");
        DataStream<PriceRuleUpdate> ruleUpdates = ruleCdc.createRaw(env);

        FlinkPriceRuleProvider ruleProv = new FlinkPriceRuleProvider();

        ruleUpdates
              .keyBy(PriceRuleUpdate::getProductId)
              .process(new KeyedProcessFunction<String, PriceRuleUpdate, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      ruleProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(PriceRuleUpdate upd, Context ctx, Collector<Void> out)
                        throws Exception {
                      ruleProv.updateRule(upd.getPriceRule());
                  }
              })
              .name("UpdatePriceRuleState");

        return ruleProv;
    }


}
