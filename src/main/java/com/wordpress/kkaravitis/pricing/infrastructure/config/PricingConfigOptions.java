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
          ConfigOptions.key("kafka.click.group-id")
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
          ConfigOptions.key("kafka.model.group-id")
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
    // Inventory source
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_INVENTORY_TOPIC =
          ConfigOptions.key("kafka.inventory.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_INVENTORY_GROUP_ID =
          ConfigOptions.key("kafka.inventory.group-id")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // PriceRule source
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_PRICERULE_TOPIC =
          ConfigOptions.key("kafka.priceRule.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_PRICERULE_GROUP_ID =
          ConfigOptions.key("kafka.priceRule.group-id")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Order source
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> KAFKA_ORDERS_TOPIC =
          ConfigOptions.key("kafka.orders.topic")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<String> KAFKA_ORDERS_GROUP_ID =
          ConfigOptions.key("kafka.orders.group-id")
                .stringType()
                .noDefaultValue();

    // ------------------------------------------------------------------------
    // Competitor HTTP API
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> COMPETITOR_API_BASE_URL =
          ConfigOptions.key("competitor.api.base-url")
                .stringType()
                .noDefaultValue();

    public static final ConfigOption<Boolean> TEST_MODE =
          ConfigOptions.key("test")
                .booleanType()
                .defaultValue(false);
}