package com.wordpress.kkaravitis.pricing.infrastructure.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.OrderEvent;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reads INSERTs/UPDATEs from the orders table via Debezium CDC and maps to OrderEvent.
 */
public class OrderCdcSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MySqlSource<String> cdcSource;

    public OrderCdcSource(OrderCdcSourceContext context) {
        this.cdcSource = MySqlSource.<String>builder()
              .hostname(context.host)
              .port(context.port)
              .databaseList(context.database)
              .tableList(context.database + "." + context.table)
              .username(context.user)
              .password(context.password)
              .startupOptions(StartupOptions.initial())
              .deserializer(new JsonDebeziumDeserializationSchema())
              .build();
    }

    /**
     * @return a stream of OrderEvent keyed by productId.
     */
    public DataStream<OrderEvent> create(StreamExecutionEnvironment env) {
        return env
              .fromSource(
                    cdcSource,
                    WatermarkStrategy.<String>forMonotonousTimestamps(),
                    "OrderCDC"
              )
              .map(json -> {
                  JsonNode after = MAPPER.readTree(json).get("after");
                  return new OrderEvent(
                        after.get("order_id").asText(),
                        after.get("product_id").asText(),
                        after.get("quantity").asInt(),
                        after.get("ts_ms").asLong()
                  );
              });
    }

    @Builder
    @Getter
    public static class OrderCdcSourceContext {
        private String host;
        private Integer port;
        private String database;
        private String table;
        private String user;
        private String password;
    }
}
