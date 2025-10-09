/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PricingEngineServiceTest {

    @Mock
    DemandMetricsRepository demandMetricsRepository;

    @Mock
    InventoryLevelRepository inventoryLevelRepository;

    @Mock
    CompetitorPriceRepository competitorPriceRepository;

    @Mock
    PriceRuleRepository priceRuleRepository;

    @Mock
    ModelInferencePricePredictor modelInferencePricePredictor;

    @Mock
    EmergencyPriceAdjustmentRepository emergencyPriceAdjustmentRepository;

    private PricingEngineService serviceUnderTest;

    @BeforeEach
    void setup() {
        serviceUnderTest = new PricingEngineService(
              demandMetricsRepository,
              inventoryLevelRepository,
              competitorPriceRepository,
              priceRuleRepository,
              modelInferencePricePredictor,
              emergencyPriceAdjustmentRepository
        );
    }

    @Test
    void computePrice_noAdjustments_returnsWeightedAverage() throws PricingException {
        // given
        String pid = "p1";
        String pname="product1";

        given(modelInferencePricePredictor.predictPrice(any()))
              .willReturn(new Money(1.12, "EUR"));
        given(competitorPriceRepository.getCompetitorPrice(pid))
              .willReturn(new CompetitorPrice(pid, pid, new Money(1.00, "EUR")));
        given(demandMetricsRepository.getDemandMetrics(pid))
              .willReturn(new DemandMetrics(pid, pid,5.0, 5.0));
        given(inventoryLevelRepository.getInventoryLevel(pid))
              .willReturn(100);
        given(emergencyPriceAdjustmentRepository.getAdjustmentFactor(pid))
              .willReturn(1.0);
        given(priceRuleRepository.getPriceRule(pid))
              .willReturn(PriceRule.defaults());

        // when
        PricingResult result = serviceUnderTest.computePrice(new Product(pid, pname));

        // then
        assertEquals(new Money(1.08, "EUR"), result.newPrice());
    }

    @Test
    void computePrice_demandAndInventoryAndEmergencyApplied() throws PricingException {
        //given
        String productId = "p2";
        String productName = "p2";
        given(modelInferencePricePredictor.predictPrice(any()))
              .willReturn(new Money(2.00, "EUR"));
        given(competitorPriceRepository.getCompetitorPrice(productId))
              .willReturn(new CompetitorPrice(productId, productId, new Money(1.00, "EUR")));
        given(demandMetricsRepository.getDemandMetrics(productId))
              .willReturn(new DemandMetrics(productId, productId,20.0, 10.0));
        given(inventoryLevelRepository.getInventoryLevel(productId))
              .willReturn(5);
        given(emergencyPriceAdjustmentRepository.getAdjustmentFactor(productId))
              .willReturn(1.5);
        PriceRule rule = new PriceRule(new Money(1.00, "EUR"), new Money(3.00, "EUR"));
        given(priceRuleRepository.getPriceRule(productId))
              .willReturn(rule);

        // when
        PricingResult result = serviceUnderTest.computePrice(new Product(productId, productName));

        // then
        assertEquals(
              new Money(2.96, "EUR"),
              result.newPrice(),
              "Should apply demand, inventory, emergency then clamp to max"
        );
    }
}