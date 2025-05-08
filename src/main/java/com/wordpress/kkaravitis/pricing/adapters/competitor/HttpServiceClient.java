package com.wordpress.kkaravitis.pricing.adapters.competitor;

/**
 * Abstraction for fetching raw JSON responses over HTTP.
 */
public interface HttpServiceClient {
    /**
     * Performs an HTTP GET request to the specified URL and returns the response body as a string.
     * @param url the full URL to GET
     * @return response body string
     * @throws Exception on network or IO errors
     */
    String get(String url) throws Exception;//TODO: Replace with application dedicated exception
}
