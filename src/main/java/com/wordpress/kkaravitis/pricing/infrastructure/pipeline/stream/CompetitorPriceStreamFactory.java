/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class CompetitorPriceStreamFactory {

    public DataStream<MetricUpdate> build (DataStream<ClickEvent> clicks, Configuration config) {
        // Async competitor price lookup
        FlinkAsyncCompetitorEnrichment asyncEnrich = new FlinkAsyncCompetitorEnrichment(config.
              get(PricingConfigOptions.COMPETITOR_API_BASE_URL));

        DataStream<String> productIds = clicks
              .map(ClickEvent::productId).name("ExtractProductId");

        SingleOutputStreamOperator<CompetitorPrice> competitorPrices = AsyncDataStream
              .unorderedWait(productIds, asyncEnrich, 2000, TimeUnit.MILLISECONDS, 50)
              .name("AsyncCompetitorEnrichment");

        return competitorPrices
              .map(c -> new MetricUpdate(c.productId(), MetricType.COMPETITOR, c))
             .name("UpdateCompetitorState");
    }

}
