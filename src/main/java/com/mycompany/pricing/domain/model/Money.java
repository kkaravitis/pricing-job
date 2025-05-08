package com.mycompany.pricing.domain.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Represents a monetary value with precision.
 * Encapsulates amount and currency, providing arithmetic operations.
 */
@Getter
@EqualsAndHashCode
@ToString
public class Money implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BigDecimal amount;
    private final String currency;

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

    public Money multiply(double factor) {
        BigDecimal result = this.amount.multiply(BigDecimal.valueOf(factor));
        return new Money(result, currency);
    }

    public Money max(Money other) {
        ensureSameCurrency(other);
        return this.amount.compareTo(other.amount) >= 0 ? this : other;
    }

    public Money min(Money other) {
        ensureSameCurrency(other);
        return this.amount.compareTo(other.amount) <= 0 ? this : other;
    }

    private void ensureSameCurrency(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException("Currency mismatch");
        }
    }

    /**
     * Check if this amount is less than another.
     */
    public boolean isLessThan(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException("Currency mismatch");
        }
        return this.amount.compareTo(other.amount) < 0;
    }

    /**
     * Check if this amount is greater than another.
     */
    public boolean isGreaterThan(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException("Currency mismatch");
        }
        return this.amount.compareTo(other.amount) > 0;
    }
}
