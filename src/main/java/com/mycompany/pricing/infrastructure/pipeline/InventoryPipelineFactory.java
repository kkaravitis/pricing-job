package com.mycompany.pricing.infrastructure.pipeline;

import com.mycompany.pricing.adapters.FlinkInventoryLevelRepository;
import com.mycompany.pricing.domain.InventoryEvent;
import com.mycompany.pricing.infrastructure.source.InventoryCdcSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class InventoryPipelineFactory {

    public void build(StreamExecutionEnvironment env) {
        InventoryCdcSource invCdc = new InventoryCdcSource(
              "db-host", 3306,
              "warehouse_db", "inventory",
              "dbuser", "dbpass"
        );
        DataStream<InventoryEvent> inventoryStream = invCdc.create(env);
        FlinkInventoryLevelRepository invProv = new FlinkInventoryLevelRepository();
        inventoryStream
              .keyBy(InventoryEvent::getProductId)
              .process(new KeyedProcessFunction<String, InventoryEvent, Void>() {
                  @Override
                  public void open(Configuration cfg) {
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
