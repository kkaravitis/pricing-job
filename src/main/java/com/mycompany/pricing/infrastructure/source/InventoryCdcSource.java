package com.mycompany.pricing.infrastructure.source;

import com.mycompany.pricing.domain.model.InventoryEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

public class InventoryCdcSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MySqlSource<String> cdc;

    public InventoryCdcSource(
          String host, int port,
          String database, String table,
          String user, String pass
    ) {
        this.cdc = MySqlSource.<String>builder()
              .hostname(host)
              .port(port)
              .databaseList(database)
              .tableList(database + "." + table)
              .username(user)
              .password(pass)
              .startupOptions(StartupOptions.initial())
              .deserializer(new JsonDebeziumDeserializationSchema())
              .build();
    }

    /** Emits raw JSON strings from Debezium. */
    public DataStream<String> createRaw(StreamExecutionEnvironment env) {
        return env
              .fromSource(cdc, WatermarkStrategy.noWatermarks(), "InventoryCDC");
    }

    /** Parses the JSON into InventoryEvent. */
    public DataStream<InventoryEvent> create(StreamExecutionEnvironment env) {
        return createRaw(env)
              .map(json -> {
                  JsonNode after = MAPPER.readTree(json).get("after");
                  String pid = after.get("product_id").asText();
                  int lvl     = after.get("level").asInt();
                  return new InventoryEvent(pid, lvl);
              });
    }
}
