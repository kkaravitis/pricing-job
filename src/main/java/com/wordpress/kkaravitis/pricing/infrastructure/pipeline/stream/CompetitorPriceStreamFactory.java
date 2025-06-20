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
