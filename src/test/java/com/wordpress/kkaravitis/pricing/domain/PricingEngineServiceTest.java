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

        given(modelInferencePricePredictor.predictPrice(any()))
              .willReturn(new Money(1.12, "USD"));
        given(competitorPriceRepository.getCompetitorPrice(pid))
              .willReturn(new CompetitorPrice(pid, new Money(1.00, "USD")));
        given(demandMetricsRepository.getDemandMetrics(pid))
              .willReturn(new DemandMetrics(pid, 5.0, 5.0));
        given(inventoryLevelRepository.getInventoryLevel(pid))
              .willReturn(100);
        given(emergencyPriceAdjustmentRepository.getAdjustmentFactor(pid))
              .willReturn(1.0);
        given(priceRuleRepository.getPriceRule(pid))
              .willReturn(PriceRule.defaults());

        // when
        PricingResult result = serviceUnderTest.computePrice(pid);

        // then
        assertEquals(new Money(1.08, "USD"), result.getNewPrice());
    }

    @Test
    void computePrice_demandAndInventoryAndEmergencyApplied() throws PricingException {
        //given
        String productId = "p2";
        given(modelInferencePricePredictor.predictPrice(any()))
              .willReturn(new Money(2.00, "USD"));
        given(competitorPriceRepository.getCompetitorPrice(productId))
              .willReturn(new CompetitorPrice(productId, new Money(1.00, "USD")));
        given(demandMetricsRepository.getDemandMetrics(productId))
              .willReturn(new DemandMetrics(productId, 20.0, 10.0));
        given(inventoryLevelRepository.getInventoryLevel(productId))
              .willReturn(5);
        given(emergencyPriceAdjustmentRepository.getAdjustmentFactor(productId))
              .willReturn(1.5);
        PriceRule rule = new PriceRule(new Money(1.00, "USD"), new Money(3.00, "USD"));
        given(priceRuleRepository.getPriceRule(productId))
              .willReturn(rule);

        // when
        PricingResult result = serviceUnderTest.computePrice(productId);

        // then
        assertEquals(
              new Money(2.96, "USD"),
              result.getNewPrice(),
              "Should apply demand, inventory, emergency then clamp to max"
        );
    }
}