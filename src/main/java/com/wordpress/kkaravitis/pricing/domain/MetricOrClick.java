package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

public sealed interface MetricOrClick extends Serializable
      permits MetricOrClick.Click, MetricOrClick.Metric {

    record Click(ClickEvent event) implements MetricOrClick { }

    record Metric(MetricUpdate update) implements MetricOrClick { }
}