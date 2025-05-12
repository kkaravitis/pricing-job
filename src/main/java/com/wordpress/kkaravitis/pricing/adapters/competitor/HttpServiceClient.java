package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.PricingException;

/**
 * Abstraction for fetching raw JSON responses over HTTP.
 */
public interface HttpServiceClient {
    /**
     * Performs an HTTP GET request to the specified URL and returns the response body as a string.
     * @param url the full URL to GET
     * @return response body string
     * @throws PricingException on network or IO errors
     */
    String get(String url) throws PricingException;
}
