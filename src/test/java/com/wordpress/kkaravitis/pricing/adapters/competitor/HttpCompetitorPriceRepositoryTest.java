/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
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