package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustment;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.OrderEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.OrderCdcSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.OrderCdcSource.OrderCdcSourceContext;
import java.time.Duration;
import java.util.List;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Detects flash‐sale spikes (≥10 orders in 1 min) and emits EmergencyPriceAdjustment.
 */
public class AnomalyDetectionPipelineFactory {

    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {
        // 1) Ingest order events via CDC
        OrderCdcSource ordersCdc = new OrderCdcSource(
              OrderCdcSourceContext.builder()
                    .host(config.get(PricingConfigOptions.ORDER_CDC_HOST))
                    .database(config.get(PricingConfigOptions.ORDER_CDC_DATABASE))
                    .port(config.get(PricingConfigOptions.ORDER_CDC_PORT))
                    .password(config.get(PricingConfigOptions.ORDER_CDC_PASSWORD))
                    .table(config.get(PricingConfigOptions.ORDER_CDC_TABLE))
                    .user(config.get(PricingConfigOptions.ORDER_CDC_USER))
                    .build()
        );
        DataStream<OrderEvent> orders = ordersCdc.create(env);

        // 2) Define a CEP pattern: ten or more events in 1 minute
        Pattern<OrderEvent, ?> flashSalePattern = Pattern.<OrderEvent>begin("start")
              .where(new SimpleCondition<>() {
                  @Override
                  public boolean filter(OrderEvent value) {
                      return true;
                  }
              })
              .times(10)                            // require at least 10 matches
              .consecutive()                       // back‐to‐back
              .within(Duration.ofMinutes(1));      // within one minute

        // 3) Apply pattern keyed by productId
        SingleOutputStreamOperator<EmergencyPriceAdjustment> adjustments =
              CEP.pattern(
                          orders.keyBy(OrderEvent::productId),
                          flashSalePattern
                    )
                    .select((PatternSelectFunction<OrderEvent, EmergencyPriceAdjustment>) pattern -> {
                        List<OrderEvent> events = pattern.get("start");
                        String pid = events.get(0).productId();
                        // e.g. increase price by 20% during flash sale
                        return new EmergencyPriceAdjustment(pid, 1.2);
                    });


        return adjustments
              .map(a -> new MetricUpdate(a.productId(), MetricType.EMERGENCY, a))
              .name("EmergencyAdjustmentUpdate");
    }
}
