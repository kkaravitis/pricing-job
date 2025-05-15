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
                return new CompetitorPrice(productId, new Money(0.0, "USD"));
            }
            JsonNode node = mapper.readTree(json);
            double price = node.get("price").asDouble();
            return new CompetitorPrice(productId, new Money(price, "USD"));
        } catch (PricingException pricingException) {
            throw pricingException;
        } catch (Exception e) {
            throw new PricingException("Failed to fetch competitor price for " + productId, e);
        }
    }
}
