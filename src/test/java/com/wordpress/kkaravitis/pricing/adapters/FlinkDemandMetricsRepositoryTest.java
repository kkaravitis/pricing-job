/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlinkDemandMetricsRepositoryTest {
    @Mock
    ValueState<DemandMetrics> state;

    FlinkDemandMetricsRepository demandMetricsRepository;


    @BeforeEach
    void setup() throws NoSuchFieldException, IllegalAccessException {
        demandMetricsRepository = new FlinkDemandMetricsRepository();

        Field f = FlinkDemandMetricsRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(demandMetricsRepository, state);
    }

    @Test
    void getDemandMetrics_whenStateIsNull_returnsDefaultZeroDemand() throws Exception {
        // given
        String pid = "product-123";
        given(state.value()).willReturn(null);
        // when
        DemandMetrics demandMetrics = demandMetricsRepository.getDemandMetrics(pid);
        // then
        assertEquals(new DemandMetrics(pid, "",0, 0), demandMetrics);
    }

    @Test
    void getDemandMetrics_whenStateNotNull_returnsStateValue() throws Exception {
        // given
        String pid = "product-123";
        DemandMetrics stateValue = new DemandMetrics(pid, pid,32.0, 54.0);
        given(state.value()).willReturn(stateValue);
        // when
        DemandMetrics demandMetrics = demandMetricsRepository.getDemandMetrics(pid);

        // then
        Mockito.verify(state, times(1)).value();
        assertEquals(stateValue, demandMetrics);
    }

    @Test
    void updateDemandMetrics_invokeStateUpdate() throws Exception {
        // given
        DemandMetrics demandMetrics = new DemandMetrics("product-123", "product-123",12.0, 32.0);
        // when
        demandMetricsRepository.updateMetrics(demandMetrics);
        // then
        verify(state, times(1)).update(demandMetrics);
    }

    @Test
    void updateDemandMetrics_throwsPricingException() throws Exception {
        IOException ioException = new IOException("test IO exception");
        DemandMetrics demandMetrics = new DemandMetrics("product-123", "product-123",12.0, 32.0);
        doThrow(ioException).when(state).update(demandMetrics);

        PricingException pricingException = assertThrows(PricingException.class, () -> {
           demandMetricsRepository.updateMetrics(demandMetrics);
        });

        assertTrue(pricingException.getCause() instanceof IOException);
        assertEquals(ioException.getMessage(), pricingException.getCause().getMessage());
        verify(state, times(1)).update(demandMetrics);


    }
}