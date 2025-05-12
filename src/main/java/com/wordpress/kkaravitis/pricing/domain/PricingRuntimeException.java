package com.wordpress.kkaravitis.pricing.domain;

/**
 * An unchecked exception for non-recoverable pricing-engine errors.
 */
public class PricingRuntimeException extends RuntimeException {
    public PricingRuntimeException(Exception exception) {
        super(exception);
    }
    public PricingRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
