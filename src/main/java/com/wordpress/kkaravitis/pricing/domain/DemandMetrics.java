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

/**
 * Holds metrics related to product demand. - currentDemand: the real-time demand rate (e.g., orders or clicks per minute). - historicalAverage: baseline demand
 * derived from historical data. These values inform pricing adjustments based on demand fluctuations.
 */
public record DemandMetrics(String productId, String productName, double currentDemand, double historicalAverage) implements Serializable {
}