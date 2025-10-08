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
package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.Money;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import java.lang.reflect.Field;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlinkCompetitorPriceRepositoryTest {

    @Mock
    private ValueState<CompetitorPrice> state;

    private FlinkCompetitorPriceRepository competitorPriceRepository;

    @BeforeEach
    void setUp() throws Exception {
        competitorPriceRepository = new FlinkCompetitorPriceRepository();
        // inject the mocked ValueState into the private 'state' field
        Field f = FlinkCompetitorPriceRepository.class.getDeclaredField("state");
        f.setAccessible(true);
        f.set(competitorPriceRepository, state);
    }

    @Test
    void getCompetitorPrice_whenStateIsNull_returnsDefaultZeroUsd() throws Exception {
        String pid = "product-123";
        when(state.value()).thenReturn(null);

        CompetitorPrice result = competitorPriceRepository.getCompetitorPrice(pid);

        assertEquals(pid, result.productId());
        assertEquals(new Money(0.0, "EUR"), result.price());
    }

    @Test
    void getCompetitorPrice_whenStateNotNull_returnsStateValue() throws Exception {
        CompetitorPrice stored = new CompetitorPrice("p2", "p2", new Money(5.25, "EUR"));
        when(state.value()).thenReturn(stored);

        CompetitorPrice result = competitorPriceRepository.getCompetitorPrice("p2");

        assertSame(stored, result);
    }

    @Test
    void updatePrice_invokesStateUpdate() throws Exception {
        CompetitorPrice cp = new CompetitorPrice("p3", "p3", new Money(3.14, "EUR"));
        competitorPriceRepository.updatePrice(cp);
        verify(state).update(cp);
    }
}