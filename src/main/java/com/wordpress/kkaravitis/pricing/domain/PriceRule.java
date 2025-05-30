/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Defines business-imposed pricing bounds for a product. - minPrice: lowest allowed price set by category managers. - maxPrice: highest allowed price set by
 * category managers. Ensures the computed price stays within safe thresholds.
 */
public record PriceRule(Money minPrice, Money maxPrice) implements Serializable {

    /**
     * Returns a rule that effectively imposes no bounds: min = 0, max = Double.MAX_VALUE in USD.
     */
    public static PriceRule defaults() {
        return new PriceRule(
              new Money(BigDecimal.ZERO, "USD"),
              new Money(BigDecimal.valueOf(Double.MAX_VALUE), "USD")
        );
    }

}