/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.PricingException;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlinkEmergencyAdjustmentRepositoryTest {

    @Mock
    private ValueState<Double> state;

    private FlinkEmergencyAdjustmentRepository repo;

    @BeforeEach
    void setUp() throws Exception {
        repo = new FlinkEmergencyAdjustmentRepository();

        Field f = FlinkEmergencyAdjustmentRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(repo, state);
    }

    @Test
    void updateAdjustment_success() throws Exception {
        // when
        repo.updateAdjustment(1.23);

        // then
        verify(state).update(1.23);
    }

    @Test
    void updateAdjustment_ioException_throwsPricingException() throws Exception {
        // given
        doThrow(new IOException("disk error"))
              .when(state).update(2.34);

        // when
        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.updateAdjustment(2.34)
        );

        // then
        assertTrue(ex.getMessage().contains(
              "Failed to update emergency adjustment factor in flink state."
        ));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("disk error", ex.getCause().getMessage());
    }

    @Test
    void getAdjustmentFactor_whenValuePresent_returnsThatValue() throws Exception {
        // given
        when(state.value()).thenReturn(4.56);

        // when
        double f = repo.getAdjustmentFactor("any-product");

        // then
        assertEquals(4.56, f);
    }

    @Test
    void getAdjustmentFactor_whenNoValue_returnsDefaultOne() throws Exception {
        // given
        when(state.value()).thenReturn(null);

        // when
        double f = repo.getAdjustmentFactor("p-xyz");

        // then
        assertEquals(1.0, f);
    }

    @Test
    void getAdjustmentFactor_ioException_throwsPricingException() throws Exception {
        // given
        when(state.value()).thenThrow(new IOException("fetch error"));

        // when
        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.getAdjustmentFactor("p-123")
        );

        // then
        assertTrue(ex.getMessage().contains(
              "Failed to fetch the emergency adjustment factor"
        ));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IOException);
        assertEquals("fetch error", ex.getCause().getMessage());
    }
}