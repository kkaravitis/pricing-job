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

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a monetary value with precision.
 * Encapsulates amount and currency, providing arithmetic operations.
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Money implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final String CURRENCY_MISMATCH = "Currency mismatch";

    private final BigDecimal amount;
    private final String currency;

    public Money() {
        this.amount = null;
        this.currency = null;
    }

    public Money(BigDecimal amount, String currency) {
        this.amount = amount.setScale(2, RoundingMode.HALF_UP);
        this.currency = currency;
    }

    public Money(double value, String currency) {
        this(BigDecimal.valueOf(value), currency);
    }

    public Money add(Money other) {
        ensureSameCurrency(other);
        return new Money(this.amount.add(other.amount), currency);
    }

    public Money subtract(Money other) {
        ensureSameCurrency(other);
        return new Money(this.amount.subtract(other.amount), currency);
    }

    public Money divide(Money other) {
        ensureSameCurrency(other);

        return new Money(this.amount.divide(other.amount), currency);
    }

    public Money abs() {
        return new Money(this.amount.abs(), currency);
    }

    public Money multiply(double factor) {
        BigDecimal result = this.amount.multiply(BigDecimal.valueOf(factor));
        return new Money(result, currency);
    }

    private void ensureSameCurrency(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(CURRENCY_MISMATCH);
        }
    }

    /**
     * Check if this amount is less than another.
     */
    public boolean isLessThan(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(CURRENCY_MISMATCH);
        }
        return this.amount.compareTo(other.amount) < 0;
    }

    /**
     * Check if this amount is greater than another.
     */
    public boolean isGreaterThan(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(CURRENCY_MISMATCH);
        }
        return this.amount.compareTo(other.amount) > 0;
    }
}
