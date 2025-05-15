package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.domain.InventoryEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.InventoryCdcSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.InventoryCdcSource.InventoryCdcSourceContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class InventoryPipelineFactory {

    public void build(StreamExecutionEnvironment env, Configuration config) {
        InventoryCdcSource invCdc = new InventoryCdcSource(
              InventoryCdcSourceContext.builder()
                    .host(config.get(PricingConfigOptions.INVENTORY_CDC_HOST))
                    .port(config.get(PricingConfigOptions.INVENTORY_CDC_PORT))
                    .database(config.get(PricingConfigOptions.INVENTORY_CDC_DATABASE))
                    .table(config.get(PricingConfigOptions.INVENTORY_CDC_TABLE))
                    .username(config.get(PricingConfigOptions.INVENTORY_CDC_USER))
                    .password(config.get(PricingConfigOptions.INVENTORY_CDC_PASSWORD))
                    .build()
        );
        DataStream<InventoryEvent> inventoryStream = invCdc.create(env);
        FlinkInventoryLevelRepository invProv = new FlinkInventoryLevelRepository();
        inventoryStream
              .keyBy(InventoryEvent::getProductId)
              .process(new KeyedProcessFunction<String, InventoryEvent, Void>() {
                  @Override
                  public void open(OpenContext openContext) {
                      invProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(InventoryEvent ie, Context ctx, Collector<Void> out)
                        throws Exception {
                      invProv.updateLevel(ie.getLevel());
                  }
              })
              .name("UpdateInventoryState");
    }



}
