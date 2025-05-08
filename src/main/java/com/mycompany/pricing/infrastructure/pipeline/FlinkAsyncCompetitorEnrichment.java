// src/main/java/com/mycompany/pricing/infrastructure/async/FlinkAsyncCompetitorEnrichment.java
package com.mycompany.pricing.infrastructure.pipeline;

import com.mycompany.pricing.domain.CompetitorPrice;
import com.mycompany.pricing.domain.CompetitorPriceRepository;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps a CompetitorPriceProvider in a non-blocking Flink AsyncFunction.
 */
public class FlinkAsyncCompetitorEnrichment
      implements AsyncFunction<String, CompetitorPrice> {

    private final CompetitorPriceRepository competitorPriceRepository;

    public FlinkAsyncCompetitorEnrichment(CompetitorPriceRepository competitorPriceRepository) {
        this.competitorPriceRepository = competitorPriceRepository;
    }

    @Override
    public void asyncInvoke(String productId, ResultFuture<CompetitorPrice> resultFuture) {
        CompletableFuture
              .supplyAsync(() -> {
                  try {
                      return competitorPriceRepository.getCompetitorPrice(productId);
                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  }
              })
              .thenAccept(cp -> resultFuture.complete(
                    Collections.singletonList(cp)
              ));
    }
}
