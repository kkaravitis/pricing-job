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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class MoneyTest {

    @Test
    void constructor_scalesToTwoDecimalsHalfUp() {
        Money m1 = new Money(new BigDecimal("1.234"), "USD");
        assertEquals(new BigDecimal("1.23"), m1.getAmount());

        Money m2 = new Money(new BigDecimal("1.235"), "USD");
        assertEquals(new BigDecimal("1.24"), m2.getAmount());
    }

    @Test
    void add_sameCurrency_sumsAmounts() {
        Money a = new Money(1.10, "EUR");
        Money b = new Money(2.25, "EUR");
        Money sum = a.add(b);
        assertEquals(new Money(3.35, "EUR"), sum);
    }

    @Test
    void add_currencyMismatch_throws() {
        Money a = new Money(1.00, "USD");
        Money b = new Money(1.00, "EUR");
        assertThrows(IllegalArgumentException.class, () -> a.add(b));
    }

    @Test
    void multiply_roundsAtEachStep() {
        // 1.00 × 0.333 = 0.333 → rounds to 0.33
        Money m = new Money(1.00, "USD").multiply(0.333);
        assertEquals(new Money(0.33, "USD"), m);
    }

    @Test
    void multiply_currencyMismatchNotApplicable() {
        // multiply doesn't inspect other Money instances
        Money m = new Money(2.00, "JPY").multiply(1.5);
        assertEquals(new Money(3.00, "JPY"), m);
    }

    @Test
    void comparisons_currencyMismatch_throws() {
        Money u = new Money(1.00, "USD");
        Money e = new Money(1.00, "EUR");
        assertThrows(IllegalArgumentException.class, () -> u.isLessThan(e));
        assertThrows(IllegalArgumentException.class, () -> u.isGreaterThan(e));
    }

    @Test
    void toString_includesCurrencyAndAmount() {
        Money m = new Money(12.50, "AUD");
        String s = m.toString();
        assertTrue(s.contains("12.50"));
        assertTrue(s.contains("AUD"));
    }
}
