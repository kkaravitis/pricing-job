package com.mycompany.pricing.adapters.competitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.pricing.domain.CompetitorPrice;
import com.mycompany.pricing.domain.Money;
import com.mycompany.pricing.domain.CompetitorPriceRepository;

/**
 * Adapter: implements the domain CompetitorPriceProvider by fetching JSON price data over HTTP.
 */
public class HttpCompetitorPriceRepository implements CompetitorPriceRepository {
    private final HttpServiceClient client;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpCompetitorPriceRepository(HttpServiceClient client, String baseUrl) {
        this.client = client;
        this.baseUrl = baseUrl;
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) {
        try {
            String url = String.format("%s/price/%s", baseUrl, productId);
            String json = client.get(url);
            JsonNode node = mapper.readTree(json);
            double price = node.get("price").asDouble();
            return new CompetitorPrice(productId, new Money(price, "USD"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch competitor price for " + productId, e);
        }
    }
}
