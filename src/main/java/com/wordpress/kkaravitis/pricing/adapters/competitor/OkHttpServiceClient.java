package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.PricingException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.Duration;

/**
 * HTTP client implementation using OkHttp for efficient, pooled connections.
 */
public class OkHttpServiceClient implements HttpServiceClient {

    private final OkHttpClient client;

    public OkHttpServiceClient() {
        this.client = new OkHttpClient.Builder()
              .connectTimeout(Duration.ofSeconds(5))
              .readTimeout(Duration.ofSeconds(5))
              .retryOnConnectionFailure(true)
              .build();
    }

    @Override
    public String get(String url) throws PricingException {
        Request request = new Request.Builder()
              .url(url)
              .get()
              .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new PricingException("HTTP " + response.code() + " for " + url);
            }
            return response.body() != null ? response.body().string() : null;
        } catch (IOException e) {
            throw new PricingException("Failed to communicate with competitor site.", e);
        }
    }
}

