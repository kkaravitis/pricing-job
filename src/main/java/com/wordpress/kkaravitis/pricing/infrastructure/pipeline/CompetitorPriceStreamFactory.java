package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

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

        SingleOutputStreamOperator<String> prodIds = clicks
              .map(ClickEvent::productId).name("ExtractProductId");

        DataStream<CompetitorPrice> competitorPrices = AsyncDataStream
              .unorderedWait(prodIds, asyncEnrich, 2000, TimeUnit.MILLISECONDS, 50)
              .name("AsyncCompetitorEnrichment");

        return competitorPrices
              .map(c -> new MetricUpdate(c.productId(), MetricType.COMPETITOR, c))
             .name("UpdateCompetitorState");
    }

}
