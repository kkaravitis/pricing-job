package com.wordpress.kkaravitis.pricing.infrastructure.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Central repository of all configuration keys (and their types/defaults)
 * used by the pricing job. These map 1:1 to entries in config.yaml.
 */
public final class PricingConfigOptions {

    private PricingConfigOptions() {
        // no instances
    }

    // ------------------------------------------------------------------------
    // Kafka general
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_BOOTSTRAP_SERVERS =
          ConfigOptions.key("kafka.bootstrap.servers")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Click events
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_CLICK_TOPIC =
          ConfigOptions.key("kafka.click.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_CLICK_GROUP_ID =
          ConfigOptions.key("kafka.click.group.id")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Model broadcast
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_MODEL_TOPIC =
          ConfigOptions.key("kafka.model.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_MODEL_GROUP_ID =
          ConfigOptions.key("kafka.model.group.id")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Pricing results sink
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_PRICING_TOPIC =
          ConfigOptions.key("kafka.pricing.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_PRICING_TXN_ID_PREFIX =
          ConfigOptions.key("kafka.pricing.transactional.id.prefix")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Alerts sink
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_ALERTS_TOPIC =
          ConfigOptions.key("kafka.alerts.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_ALERTS_TXN_ID_PREFIX =
          ConfigOptions.key("kafka.alerts.transactional.id.prefix")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Inventory CDC (Debezium)
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> INVENTORY_CDC_HOST =
          ConfigOptions.key("inventoryCdc.host")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<Integer> INVENTORY_CDC_PORT =
          ConfigOptions.key("inventoryCdc.port")
                .intType()
                .noDefaultValue();

    public static final ConfigOption<String> INVENTORY_CDC_DATABASE =
          ConfigOptions.key("inventoryCdc.database")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> INVENTORY_CDC_TABLE =
          ConfigOptions.key("inventoryCdc.table")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> INVENTORY_CDC_USER =
          ConfigOptions.key("inventoryCdc.user")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> INVENTORY_CDC_PASSWORD =
          ConfigOptions.key("inventoryCdc.password")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // PriceRule CDC (Debezium)
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> PRICERULE_CDC_HOST =
          ConfigOptions.key("priceRuleCdc.host")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<Integer> PRICERULE_CDC_PORT =
          ConfigOptions.key("priceRuleCdc.port")
                .intType()
                .noDefaultValue();

    public static final ConfigOption<String> PRICERULE_CDC_DATABASE =
          ConfigOptions.key("priceRuleCdc.database")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> PRICERULE_CDC_TABLE =
          ConfigOptions.key("priceRuleCdc.table")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> PRICERULE_CDC_USER =
          ConfigOptions.key("priceRuleCdc.user")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> PRICERULE_CDC_PASSWORD =
          ConfigOptions.key("priceRuleCdc.password")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Order CDC (Debezium + CEP)
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> ORDER_CDC_HOST =
          ConfigOptions.key("orderCdc.host")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<Integer> ORDER_CDC_PORT =
          ConfigOptions.key("orderCdc.port")
                .intType()
                .noDefaultValue();

    public static final ConfigOption<String> ORDER_CDC_DATABASE =
          ConfigOptions.key("orderCdc.database")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> ORDER_CDC_TABLE =
          ConfigOptions.key("orderCdc.table")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> ORDER_CDC_USER =
          ConfigOptions.key("orderCdc.user")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> ORDER_CDC_PASSWORD =
          ConfigOptions.key("orderCdc.password")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Competitor HTTP API
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> COMPETITOR_API_BASE_URL =
          ConfigOptions.key("competitor.api.base-url")
                .stringType()
                .noDefaultValue();
}