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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpServiceClient;
import com.wordpress.kkaravitis.pricing.adapters.competitor.OkHttpServiceClient;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.Money;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * Wraps a CompetitorPriceProvider in a non-blocking Flink AsyncFunction.
 */
public class FlinkAsyncCompetitorEnrichment
      extends RichAsyncFunction<String, CompetitorPrice> {

    private transient HttpCompetitorPriceRepository competitorPriceRepository;
    private final String baseUrl;

    public FlinkAsyncCompetitorEnrichment(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public void open(OpenContext openContext) {
        HttpServiceClient httpServiceClient = new OkHttpServiceClient();
        this.competitorPriceRepository = new HttpCompetitorPriceRepository(httpServiceClient, baseUrl);
    }

    @Override
    public void asyncInvoke(String productId, ResultFuture<CompetitorPrice> resultFuture) {
        CompletableFuture
              .supplyAsync(() -> {
                  try {
                      return competitorPriceRepository.getCompetitorPrice(productId);
                  } catch (Exception exception) {
                      return new CompetitorPrice(productId, new Money(0.0, "USD"));
                  }
              })
              .thenAccept(competitorPrice -> resultFuture.complete(
                    Collections.singletonList(competitorPrice)
              ));
    }
}
