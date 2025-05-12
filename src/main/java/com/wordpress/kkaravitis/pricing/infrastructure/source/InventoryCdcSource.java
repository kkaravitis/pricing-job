package com.wordpress.kkaravitis.pricing.infrastructure.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.InventoryEvent;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class InventoryCdcSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MySqlSource<String> dbSource;

    public InventoryCdcSource(
          InventoryCdcSourceContext context
    ) {
        this.dbSource = MySqlSource.<String>builder()
              .hostname(context.host)
              .port(context.port)
              .databaseList(context.database)
              .tableList(context.database + "." + context.table)
              .username(context.username)
              .password(context.password)
              .startupOptions(StartupOptions.initial())
              .deserializer(new JsonDebeziumDeserializationSchema())
              .build();
    }

    /**
     * Parses the JSON into InventoryEvent.
     */
    public DataStream<InventoryEvent> create(StreamExecutionEnvironment env) {
        return env
              .fromSource(dbSource, WatermarkStrategy.noWatermarks(), "InventoryCDC")
              .map(json -> {
                  JsonNode after = MAPPER.readTree(json).get("after");
                  String pid = after.get("product_id").asText();
                  int lvl = after.get("level").asInt();
                  return new InventoryEvent(pid, lvl);
              });
    }

    @Builder
    @Getter
    public static class InventoryCdcSourceContext {
        private String host;
        private Integer port;
        private String database;
        private String table;
        private String username;
        private String password;
    }
}
