/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

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
