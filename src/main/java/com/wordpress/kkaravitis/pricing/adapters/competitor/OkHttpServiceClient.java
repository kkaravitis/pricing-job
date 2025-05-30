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
            int code = response.code();
            if (code == 404) {
                return null;
            }
            if (!response.isSuccessful()) {
                throw new PricingException("HTTP " + response.code() + " for " + url);
            }
            return response.body() != null ? response.body().string() : null;
        } catch (IOException e) {
            throw new PricingException("Failed to communicate with competitor site.", e);
        }
    }
}

