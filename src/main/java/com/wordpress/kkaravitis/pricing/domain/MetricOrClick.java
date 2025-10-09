/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

public sealed interface MetricOrClick extends Serializable
      permits MetricOrClick.Click, MetricOrClick.Metric {

    record Click(ClickEvent event) implements MetricOrClick { }

    record Metric(MetricUpdate update) implements MetricOrClick { }
}