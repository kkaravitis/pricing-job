/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkEmergencyAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustment;
import com.wordpress.kkaravitis.pricing.domain.InventoryEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricOrClick;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.domain.PricingEngineService;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.domain.Product;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A single operator that:
 *   • receives either a click or a metric update (MetricOrClick)
 *   • updates Flink-backed repositories on metric updates
 *   • on clicks, pulls all repos + model → invokes PricingEngineService
 */
public class UnifiedPricingFunction
      extends KeyedBroadcastProcessFunction<
            String,              // key = productId
            MetricOrClick,       // left: clicks or metric updates
            byte[],              // broadcast: model bytes
            PricingResult>       // output
{

    private transient MlModelAdapter mlModelAdapter;
    private transient FlinkDemandMetricsRepository demandMetricsRepository;
    private transient FlinkInventoryLevelRepository inventoryLevelRepository;
    private transient FlinkCompetitorPriceRepository competitorPriceRepository;
    private transient FlinkPriceRuleRepository priceRuleRepository;
    private transient FlinkEmergencyAdjustmentRepository emergencyAdjustmentRepository;

    // for alerts
    private transient ValueState<Money> lastPriceState;

    // core DDD service
    private transient PricingEngineService engine;

    @Override
    public void open(OpenContext ctx) {

        mlModelAdapter = new MlModelAdapter();
        mlModelAdapter.initialize();

        RuntimeContext runtimeContext = getRuntimeContext();

        demandMetricsRepository = new FlinkDemandMetricsRepository();
        demandMetricsRepository.initializeState(getRuntimeContext());

        inventoryLevelRepository = new FlinkInventoryLevelRepository();
        inventoryLevelRepository.initializeState(runtimeContext);

        competitorPriceRepository = new FlinkCompetitorPriceRepository();
        competitorPriceRepository.initializeState(runtimeContext);

        priceRuleRepository = new FlinkPriceRuleRepository();
        priceRuleRepository.initializeState(runtimeContext);

        emergencyAdjustmentRepository = new FlinkEmergencyAdjustmentRepository();
        emergencyAdjustmentRepository.initializeState(runtimeContext);

        lastPriceState = runtimeContext.getState(
              new ValueStateDescriptor<>("lastPrice", Money.class)
        );

        // pricing service depends only on repository interfaces + adapter
        engine = new PricingEngineService(
              demandMetricsRepository,
              inventoryLevelRepository,
              competitorPriceRepository,
              priceRuleRepository,
              mlModelAdapter,
              emergencyAdjustmentRepository
        );
    }

    // Broadcast handler: update adapter with new model
    @Override
    public void processBroadcastElement(
          byte[] bytes,
          Context ctx,
          Collector<PricingResult> out
    ) {
        mlModelAdapter.updateModelBytes(bytes);
    }

    // Combined handler: either a click or a metric update
    @Override
    public void processElement(
          MetricOrClick mc,
          ReadOnlyContext ctx,
          Collector<PricingResult> out
    ) throws Exception {
        Optional<Product> optional = resolveProduct(mc);

        if(optional.isEmpty()) {
            return;
        }

        PricingResult pr = engine.computePrice(optional.get());

        sendAlertsForPriceSpikes(pr, ctx);

        out.collect(pr);

        lastPriceState.update(pr.newPrice());
    }

    private Optional<Product> resolveProduct(MetricOrClick mc) throws PricingException {
        Product product = null;
        if (mc instanceof MetricOrClick.Metric m) {
            switch (m.update().type()) {
                case DEMAND  -> {
                    DemandMetrics demandMetrics = (DemandMetrics) m.update().payload();
                    demandMetricsRepository.updateMetrics(demandMetrics);
                }
                case INVENTORY -> {
                    InventoryEvent inventoryEvent = (InventoryEvent) m.update().payload();
                    inventoryLevelRepository.updateLevel(inventoryEvent.quantity());
                    product = new Product(inventoryEvent.productId(), inventoryEvent.productName());
                }
                case COMPETITOR -> {
                    CompetitorPrice competitorPrice = (CompetitorPrice) m.update().payload();
                    competitorPriceRepository.updatePrice(competitorPrice);
                }
                case RULE -> {
                    PriceRuleUpdate priceRuleUpdate = (PriceRuleUpdate) m.update().payload();
                    priceRuleRepository.updateRule(priceRuleUpdate.priceRule());
                    product = new Product(priceRuleUpdate.productId(), priceRuleUpdate.productName());
                }
                case EMERGENCY -> {
                    EmergencyPriceAdjustment emergencyPriceAdjustment = (EmergencyPriceAdjustment) m.update().payload();
                    emergencyAdjustmentRepository.updateAdjustment(emergencyPriceAdjustment.adjustmentFactor());
                    product = new Product(emergencyPriceAdjustment.productId(), emergencyPriceAdjustment.productName());
                }
            }
        } else {
            ClickEvent click = ((MetricOrClick.Click) mc).event();
            product = new Product(click.productId(), click.productName());
        }
        return Optional.ofNullable(product);
    }

    private void sendAlertsForPriceSpikes(PricingResult pr,  ReadOnlyContext ctx) throws IOException {
        Money prev = lastPriceState.value();
        if(prev != null) {
            BigDecimal change = pr.newPrice().getAmount()
                  .subtract(prev.getAmount())
                  .divide(prev.getAmount(), RoundingMode.HALF_UP);
            if (change.compareTo(BigDecimal.valueOf(0.5)) > 0) {
                ctx.output(PricingEnginePipelineFactory.ALERT_TAG, pr);
            }
        }
    }
}