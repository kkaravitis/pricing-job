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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingException;

/**
 * Adapter: implements the domain CompetitorPriceProvider by fetching JSON price data over HTTP.
 */
public class HttpCompetitorPriceRepository implements CompetitorPriceRepository {
    private final transient HttpServiceClient client;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpCompetitorPriceRepository(HttpServiceClient client, String baseUrl) {
        this.client = client;
        this.baseUrl = baseUrl;
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) throws PricingException {
        try {
            String url = String.format("%s/price/%s", baseUrl, productId);
            String json = client.get(url);
            if (json == null) {
                // 404 or empty → treat as zero USD
                return new CompetitorPrice(productId, "", new Money(0.0, "EUR"));
            }
            JsonNode node = mapper.readTree(json);
            double price = node.get("price").asDouble();
            return new CompetitorPrice(productId, "", new Money(price, "EUR"));
        } catch (PricingException pricingException) {
            throw pricingException;
        } catch (Exception e) {
            throw new PricingException("Failed to fetch competitor price for " + productId, e);
        }
    }
}
