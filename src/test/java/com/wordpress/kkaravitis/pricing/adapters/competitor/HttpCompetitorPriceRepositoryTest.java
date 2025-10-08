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
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HttpCompetitorPriceRepositoryTest {

    @Mock
    private HttpServiceClient client;

    private HttpCompetitorPriceRepository repo;

    @BeforeEach
    void setUp() {
        repo = new HttpCompetitorPriceRepository(client, "http://api.example.com");
    }

    @Test
    void getCompetitorPrice_validJson_returnsParsedPrice() throws PricingException {
        String pid = "xyz";
        String json = "{ \"price\": 42.5 }";
        when(client.get("http://api.example.com/price/" + pid)).thenReturn(json);

        CompetitorPrice cp = repo.getCompetitorPrice(pid);

        assertEquals(pid, cp.productId());
        assertEquals(new Money(42.5, "EUR"), cp.price());
    }

    @Test
    void getCompetitorPrice_clientThrows_wrappedInPricingException() throws PricingException {
        String pid = "error";
        when(client.get(anyString())).thenThrow(new RuntimeException("HTTP fail"));

        PricingException ex = assertThrows(
              PricingException.class,
              () -> repo.getCompetitorPrice(pid)
        );
        assertTrue(ex.getMessage().contains("Failed to fetch competitor price for " + pid));
        assertNotNull(ex.getCause());
    }
}