package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkEmergencyAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingEngineService;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

    /**
     * side-output tag for large price jumps
     **/
    public static final OutputTag<PricingResult> ALERT_TAG =
          new OutputTag<PricingResult>("price-alerts"){};

    private static final long ONE_MINUTE = 60_000L;

    private final FlinkPriceRuleRepository priceRuleRepository;
    private final FlinkDemandMetricsRepository demandMetricsRepository;
    private final FlinkInventoryLevelRepository inventoryLevelRepository;
    private final FlinkCompetitorPriceRepository flinkCompetitorPriceRepository;
    private final FlinkEmergencyAdjustmentRepository emergencyAdjustmentRepository;
    private final MlModelAdapter mlModelAdapter;
    private transient PricingEngineService pricingEngineService;
    private transient ValueState<Money> lastPriceState;

    public PricingWithModelBroadcastFunction(
          FlinkPriceRuleRepository priceRuleRepository,
          FlinkDemandMetricsRepository demandMetricsRepository,
          FlinkInventoryLevelRepository inventoryLevelRepository,
          FlinkCompetitorPriceRepository flinkCompetitorPriceRepository,
          FlinkEmergencyAdjustmentRepository emergencyAdjustmentRepository,
          MlModelAdapter mlModelAdapter
    ) {
        this.priceRuleRepository = priceRuleRepository;
        this.demandMetricsRepository = demandMetricsRepository;
        this.inventoryLevelRepository = inventoryLevelRepository;
        this.flinkCompetitorPriceRepository = flinkCompetitorPriceRepository;
        this.emergencyAdjustmentRepository = emergencyAdjustmentRepository;
        this.mlModelAdapter = mlModelAdapter;

    }

    @Override
    public void open(Configuration parameters) {
        priceRuleRepository.initializeState(getRuntimeContext());
        demandMetricsRepository.initializeState(getRuntimeContext());
        inventoryLevelRepository.initializeState(getRuntimeContext());
        flinkCompetitorPriceRepository.initializeState(getRuntimeContext());
        emergencyAdjustmentRepository.initializeState(getRuntimeContext());

        pricingEngineService = new PricingEngineService(
              demandMetricsRepository,
              inventoryLevelRepository,
              flinkCompetitorPriceRepository,
              priceRuleRepository,
              mlModelAdapter,
              emergencyAdjustmentRepository
        );

        // initialize all repositories…
        ValueStateDescriptor<Money> lastPriceDesc =
              new ValueStateDescriptor<>("lastPrice", Money.class);
        lastPriceState = getRuntimeContext().getState(lastPriceDesc);
    }

    // receive model bytes via broadcast and update broadcast state
    @Override
    public void processBroadcastElement(
          byte[] modelBytes,
          Context ctx,
          Collector<PricingResult> out
    ) {
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
        PricingResult result = pricingEngineService.computePrice(productId);
        Money previous = lastPriceState.value();
        if (previous != null) {
            BigDecimal change = result.getNewPrice().getAmount()
                  .subtract(previous.getAmount())
                  .divide(previous.getAmount(), RoundingMode.HALF_UP);
            if (change.compareTo(BigDecimal.valueOf(0.5)) > 0) {
                ctx.output(ALERT_TAG, result);
            }
        }
        out.collect(result);

        // 2) schedule the next timer
        long nextTimer = timestamp + ONE_MINUTE;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }
}
