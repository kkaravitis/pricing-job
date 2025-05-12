package com.wordpress.kkaravitis.pricing.domain;

/**
 * A generic exception for all pricing‐engine errors.
 */
public class PricingException extends Exception {
    public PricingException(String message) {
        super(message);
    }
    public PricingException(String message, Throwable cause) {
        super(message, cause);
    }
}
