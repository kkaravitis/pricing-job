package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.domain.PricingEngineService;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;

/**
 * Combines click events (keyed), per‐product price rules (keyed state),
 * and a single global ML model (broadcast state) into pricing results.
 */
public class PricingWithModelBroadcastFunction
      extends KeyedBroadcastProcessFunction<
      String,           // key type: productId
      ClickEvent,       // main stream
      byte[],           // broadcast stream element: model bytes
      PricingResult>    // output
{

    private static final long ONE_MINUTE = 60_000L;

    /** Descriptor for the broadcast‐state of the ML model bytes */
    public static final MapStateDescriptor<String, byte[]> MODEL_DESCRIPTOR =
          new MapStateDescriptor<>("model-bytes", String.class, byte[].class);

    private final FlinkPriceRuleRepository priceRuleRepository;
    private final FlinkDemandMetricsRepository demandMetricsRepository;
    private final FlinkInventoryLevelRepository inventoryLevelRepository;
    private final FlinkCompetitorPriceRepository flinkCompetitorPriceRepository;
    private final MlModelAdapter mlModelAdapter;
    private transient PricingEngineService pricingEngineService;

    public PricingWithModelBroadcastFunction(
          FlinkPriceRuleRepository priceRuleRepository,
          FlinkDemandMetricsRepository demandMetricsRepository,
          FlinkInventoryLevelRepository inventoryLevelRepository,
          FlinkCompetitorPriceRepository flinkCompetitorPriceRepository,
          MlModelAdapter mlModelAdapter
    ) {
        this.priceRuleRepository = priceRuleRepository;
        this.demandMetricsRepository = demandMetricsRepository;
        this.inventoryLevelRepository = inventoryLevelRepository;
        this.flinkCompetitorPriceRepository = flinkCompetitorPriceRepository;
        this.mlModelAdapter = mlModelAdapter;

    }

    @Override
    public void open(Configuration parameters) {
        priceRuleRepository.initializeState(getRuntimeContext());
        demandMetricsRepository.initializeState(getRuntimeContext());
        inventoryLevelRepository.initializeState(getRuntimeContext());
        flinkCompetitorPriceRepository.initializeState(getRuntimeContext());

        pricingEngineService = new PricingEngineService(
              demandMetricsRepository,
              inventoryLevelRepository,
              flinkCompetitorPriceRepository,
              priceRuleRepository,
              mlModelAdapter
        );
    }

    // receive model bytes via broadcast and update broadcast state
    @Override
    public void processBroadcastElement(
          byte[] modelBytes,
          Context ctx,
          Collector<PricingResult> out
    ) throws Exception {
        mlModelAdapter.updateModelBytes(modelBytes);
    }

    // process each click event
    @Override
    public void processElement(
          ClickEvent click,
          ReadOnlyContext ctx,
          Collector<PricingResult> out
    ) throws Exception {
        String productId = click.getProductId();
        PricingResult result = pricingEngineService.computePrice(productId);
        out.collect(result);

        TimerService timerService = ctx.timerService();
        long nextTimer = timerService.currentProcessingTime() + ONE_MINUTE;
        timerService.registerProcessingTimeTimer(nextTimer);
    }

    @Override
    public void onTimer(long timestamp,
          KeyedBroadcastProcessFunction<String, ClickEvent,
                byte[], PricingResult>.OnTimerContext ctx,
          Collector<PricingResult> out) throws Exception {

        String productId = ctx.getCurrentKey();

        // 1) recompute & emit price on timer
        PricingResult result = pricingEngineService.computePrice(productId);
        out.collect(result);

        // 2) schedule the next timer
        long nextTimer = timestamp + ONE_MINUTE;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }
}
