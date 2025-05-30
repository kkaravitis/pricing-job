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
package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import java.io.Serializable;

/**
 * Abstraction over a deserialized ML model capable of scoring PricingContext.
 * Returns a Money object to preserve currency precision and context.
 */
public interface TransformedModel extends Serializable {
    /**
     * Predicts a price given the pricing context and returns it as Money.
     */
    Money predict(PricingContext context);
}
