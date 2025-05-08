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
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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

    /** Descriptor for the broadcast‐state of the ML model bytes */
    public static final MapStateDescriptor<String, byte[]> MODEL_DESCRIPTOR =
          new MapStateDescriptor<>("model-bytes", String.class, byte[].class);

    private final FlinkPriceRuleRepository ruleProvider;
    private final FlinkDemandMetricsRepository demandProvider;
    private final FlinkInventoryLevelRepository inventoryProvider;
    private final FlinkCompetitorPriceRepository competitorProvider;
    private final MlModelAdapter modelPort;
    private transient PricingEngineService pricingService;

    public PricingWithModelBroadcastFunction(
          FlinkPriceRuleRepository ruleProvider,
          FlinkDemandMetricsRepository demandProvider,
          FlinkInventoryLevelRepository inventoryProvider,
          FlinkCompetitorPriceRepository competitorProvider,
          MlModelAdapter modelPort
    ) {
        this.ruleProvider       = ruleProvider;
        this.demandProvider     = demandProvider;
        this.inventoryProvider  = inventoryProvider;
        this.competitorProvider = competitorProvider;
        this.modelPort          = modelPort;

    }

    @Override
    public void open(Configuration parameters) {

        // initialize providers
        ruleProvider.initializeState(getRuntimeContext());
        demandProvider.initializeState(getRuntimeContext());
        inventoryProvider.initializeState(getRuntimeContext());
        competitorProvider.initializeState(getRuntimeContext());


        // prepare pricing service with placeholders; real rule+model applied per event
        pricingService = new PricingEngineService(
              demandProvider,
              inventoryProvider,
              competitorProvider,
              ruleProvider,
              modelPort
        );
    }

    // receive model bytes via broadcast and update broadcast state
    @Override
    public void processBroadcastElement(
          byte[] modelBytes,
          Context ctx,
          Collector<PricingResult> out
    ) throws Exception {
        modelPort.updateModelBytes(modelBytes);
    }

    // process each click event
    @Override
    public void processElement(
          ClickEvent click,
          ReadOnlyContext ctx,
          Collector<PricingResult> out
    ) throws Exception {
        String productId = click.getProductId();

        PricingResult result = pricingService.computePrice(productId);

        out.collect(result);
    }
}
