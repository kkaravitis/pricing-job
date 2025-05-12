package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpServiceClient;
import com.wordpress.kkaravitis.pricing.adapters.competitor.OkHttpServiceClient;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import com.wordpress.kkaravitis.pricing.domain.PricingRuntimeException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
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
    public void open(Configuration parameters) {
        HttpServiceClient httpServiceClient = new OkHttpServiceClient();
        this.competitorPriceRepository = new HttpCompetitorPriceRepository(httpServiceClient, baseUrl);
    }

    @Override
    public void asyncInvoke(String productId, ResultFuture<CompetitorPrice> resultFuture) {
        CompletableFuture
              .supplyAsync(() -> {
                  try {
                      return competitorPriceRepository.getCompetitorPrice(productId);
                  } catch (PricingException e) {
                      throw new PricingRuntimeException(e);
                  }
              })
              .thenAccept(cp -> resultFuture.complete(
                    Collections.singletonList(cp)
              ));
    }
}
