package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.InventoryEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.InventoryCdcSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.InventoryCdcSource.InventoryCdcSourceContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class InventoryPipelineFactory {
    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {
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

        return inventoryStream
              .map(i -> new MetricUpdate(i.productId(), MetricType.INVENTORY, i))
              .name("InventoryUpdate");
    }
}
