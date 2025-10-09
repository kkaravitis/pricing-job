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
