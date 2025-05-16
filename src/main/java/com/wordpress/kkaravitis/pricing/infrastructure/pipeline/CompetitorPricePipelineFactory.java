package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CompetitorPricePipelineFactory {

    public void build (DataStream<ClickEvent> clicks, Configuration config) {
        // Async competitor price lookup
        FlinkAsyncCompetitorEnrichment asyncEnrich = new FlinkAsyncCompetitorEnrichment(config.
              get(PricingConfigOptions.COMPETITOR_API_BASE_URL));

        SingleOutputStreamOperator<String> prodIds = clicks
              .map(ClickEvent::productId).name("ExtractProductId");

        DataStream<CompetitorPrice> competitorPrices = AsyncDataStream
              .unorderedWait(prodIds, asyncEnrich, 2000, TimeUnit.MILLISECONDS, 50)
              .name("AsyncCompetitorEnrichment");

        FlinkCompetitorPriceRepository flinkCompetitorPriceRepository = new FlinkCompetitorPriceRepository();

        competitorPrices
              .keyBy(CompetitorPrice::productId)
              .process(new KeyedProcessFunction<String, CompetitorPrice, Void>() {
                  @Override
                  public void open(OpenContext openContext) {
                      flinkCompetitorPriceRepository.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(CompetitorPrice cp, Context ctx, Collector<Void> out)
                        throws Exception {
                      flinkCompetitorPriceRepository.updatePrice(cp);
                  }
              }).name("UpdateCompetitorState");
    }

}
