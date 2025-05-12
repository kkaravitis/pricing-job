package com.wordpress.kkaravitis.pricing.infrastructure.source;

import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reads INSERT/UPDATE/DELETE from price_rules table via Debezium CDC,
 * maps into PriceRuleUpdate, and broadcasts.
 */
public class PriceRuleCdcSource {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MySqlSource<String> cdcSource;

    public PriceRuleCdcSource(PriceRuleCdcSourceContext context) {

        this.cdcSource = MySqlSource.<String>builder()
              .hostname(context.hostname)
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
     * Returns the raw stream of PriceRuleUpdate events (unbroadcast).
     */
    public DataStream<PriceRuleUpdate> create(StreamExecutionEnvironment env) {
        return env
              .fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "PriceRuleCDC")
              .map(json -> {
                  JsonNode after = MAPPER.readTree(json).get("after");
                  String pid = after.get("product_id").asText();
                  double min = after.get("min_price").asDouble();
                  double max = after.get("max_price").asDouble();
                  return new PriceRuleUpdate(
                        pid,
                        new PriceRule(new Money(min, "USD"), new Money(max, "USD"))
                  );
              });
    }

    @Builder
    @Getter
    public static class PriceRuleCdcSourceContext {
        private String hostname;
        private Integer port;
        private String database;
        private String table;
        private String username;
        private String password;
    }
}
