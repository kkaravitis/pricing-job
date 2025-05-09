package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.HttpCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.OkHttpServiceClient;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CompetitorPricePipelineFactory {

    public void build (DataStream<ClickEvent> clicks) {
        // Async competitor price lookup
        HttpCompetitorPriceRepository httpProv =
              new HttpCompetitorPriceRepository(new OkHttpServiceClient(), "http://api.example.com");//TODO: Pass from configuration file

        FlinkAsyncCompetitorEnrichment asyncEnrich = new FlinkAsyncCompetitorEnrichment(httpProv);

        SingleOutputStreamOperator<String> prodIds = clicks
              .map(ClickEvent::getProductId).name("ExtractProductId");

        DataStream<CompetitorPrice> competitorPrices = AsyncDataStream
              .unorderedWait(prodIds, asyncEnrich, 2000, TimeUnit.MILLISECONDS, 50)
              .name("AsyncCompetitorEnrichment");

        FlinkCompetitorPriceRepository priceProv = new FlinkCompetitorPriceRepository();

        competitorPrices
              .keyBy(CompetitorPrice::getProductId)
              .process(new KeyedProcessFunction<String, CompetitorPrice, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      priceProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(CompetitorPrice cp, Context ctx, Collector<Void> out)
                        throws Exception {
                      priceProv.updatePrice(cp);
                  }
              }).name("UpdateCompetitorState");
    }

}
