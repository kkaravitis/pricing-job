/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpServiceClient;
import com.wordpress.kkaravitis.pricing.adapters.competitor.OkHttpServiceClient;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.Money;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * Wraps a CompetitorPriceProvider in a non-blocking Flink AsyncFunction.
 */
@Slf4j
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
                      CompetitorPrice competitorPrice = competitorPriceRepository.getCompetitorPrice(productId);
                      log.info("[COMPETITOR] got from competitor site: {}", competitorPrice);
                      return competitorPrice;
                  } catch (Exception exception) {
                      log.error("Failed to retrieve competitor's price", exception);
                      return new CompetitorPrice(productId, "", new Money(0.0, "EUR"));
                  }
              })
              .thenAccept(competitorPrice -> resultFuture.complete(
                    Collections.singletonList(competitorPrice)
              ));
    }
}
