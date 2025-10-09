/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class MoneyTest {

    @Test
    void constructor_scalesToTwoDecimalsHalfUp() {
        Money m1 = new Money(new BigDecimal("1.234"), "EUR");
        assertEquals(new BigDecimal("1.23"), m1.getAmount());

        Money m2 = new Money(new BigDecimal("1.235"), "EUR");
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
        Money a = new Money(1.00, "EUR");
        Money b = new Money(1.00, "GBP");
        assertThrows(IllegalArgumentException.class, () -> a.add(b));
    }

    @Test
    void multiply_roundsAtEachStep() {
        // 1.00 × 0.333 = 0.333 → rounds to 0.33
        Money m = new Money(1.00, "EUR").multiply(0.333);
        assertEquals(new Money(0.33, "EUR"), m);
    }

    @Test
    void multiply_currencyMismatchNotApplicable() {
        // multiply doesn't inspect other Money instances
        Money m = new Money(2.00, "JPY").multiply(1.5);
        assertEquals(new Money(3.00, "JPY"), m);
    }

    @Test
    void comparisons_currencyMismatch_throws() {
        Money u = new Money(1.00, "EUR");
        Money e = new Money(1.00, "GBP");
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
