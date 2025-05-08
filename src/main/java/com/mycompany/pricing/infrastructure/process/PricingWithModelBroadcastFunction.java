package com.mycompany.pricing.infrastructure.process;

import com.mycompany.pricing.domain.model.ClickEvent;
import com.mycompany.pricing.domain.model.Money;
import com.mycompany.pricing.domain.model.PriceRule;
import com.mycompany.pricing.domain.model.PricingContext;
import com.mycompany.pricing.domain.model.PricingResult;
import com.mycompany.pricing.domain.model.Product;
import com.mycompany.pricing.domain.service.PricingEngineService;
import com.mycompany.pricing.infrastructure.provider.FlinkCompetitorPriceProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkDemandMetricsProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkInventoryProvider;
import com.mycompany.pricing.infrastructure.provider.FlinkPriceRuleProvider;
import com.mycompany.pricing.infrastructure.provider.ModelDeserializer;
import com.mycompany.pricing.infrastructure.provider.TransformedModel;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
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

    private final FlinkPriceRuleProvider  ruleProvider;
    private final FlinkDemandMetricsProvider demandProvider;
    private final FlinkInventoryProvider     inventoryProvider;
    private final FlinkCompetitorPriceProvider competitorProvider;
    private final PriceRule defaultRule;
    private transient byte[] modelBytes;
    private transient PricingEngineService pricingService;

    public PricingWithModelBroadcastFunction(
          FlinkPriceRuleProvider ruleProvider,
          FlinkDemandMetricsProvider demandProvider,
          FlinkInventoryProvider inventoryProvider,
          FlinkCompetitorPriceProvider competitorProvider,
          PriceRule defaultRule
    ) {
        this.ruleProvider       = ruleProvider;
        this.demandProvider     = demandProvider;
        this.inventoryProvider  = inventoryProvider;
        this.competitorProvider = competitorProvider;
        this.defaultRule        = defaultRule;
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
              pid -> defaultRule,       // placeholder rule
              ctx -> defaultRule.getMinPrice() // placeholder price
        );

        // model not yet loaded
        modelBytes = null;
    }

    // receive model bytes via broadcast and update broadcast state
    @Override
    public void processBroadcastElement(
          byte[] bytes,
          Context ctx,
          Collector<PricingResult> out
    ) throws Exception {
        ctx.getBroadcastState(MODEL_DESCRIPTOR).put("current-model", bytes);
    }

    // process each click event
    @Override
    public void processElement(
          ClickEvent click,
          ReadOnlyContext ctx,
          Collector<PricingResult> out
    ) throws Exception {
        String productId = click.getProductId();

        // 1) read or update ruleState from provider
        PriceRule rule = ruleProvider.getPriceRule(productId);
        if (rule == null) {
            rule = defaultRule;
        }

        // 2) load model bytes once
        if (modelBytes == null) {
            ReadOnlyBroadcastState<String, byte[]> mb =
                  ctx.getBroadcastState(MODEL_DESCRIPTOR);
            modelBytes = mb.get("current-model");
        }
        TransformedModel model = ModelDeserializer.deserialize(modelBytes);

        // 3) build domain context
        PricingContext pctx = new PricingContext(
              new Product(productId),
              demandProvider.getDemandMetrics(productId),
              inventoryProvider.getInventoryLevel(productId),
              competitorProvider.getCompetitorPrice(productId),
              rule
        );

        // 4) run ML and pricing service
        Money mlBase = model.predict(pctx);
        PricingResult result = pricingService.computePrice(pctx, mlBase);

        out.collect(result);
    }
}
