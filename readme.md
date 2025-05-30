# Dynamic Pricing Flink Application

A **Dynamic Pricing Engine** continuously adjusts product prices in real time based on evolving business signals—such as live customer demand, inventory levels, competitor pricing, and machine‑learning model predictions. By reacting to market dynamics in sub‑second latency, retailers and marketplaces can optimize revenue and inventory turnover while maintaining competitive positioning.

This implementation uses **Apache Flink 2.0.0** to deliver a scalable, fault‑tolerant pricing service.

## Requirements

### Goals

1. **Real-Time Responsiveness**
   - Prices must update within seconds of new events (clicks, orders, inventory changes, competitor price fluctuations).
2. **Revenue Optimization**
   - Maximize revenue by increasing prices on high demand and preventing stockouts via price incentives.
3. **Competitive Positioning**
   - Stay aligned with or undercut competitor pricing in real time.
4. **Inventory Management**
   - Adjust prices to accelerate the sale of slow-moving items and preserve scarce inventory.
5. **Flash Sale & Anomaly Handling**
   - Detect unusual order spikes (flash sales) and apply emergency price multipliers to capture additional margin.

### Functional Requirements

1. **Data Ingestion & Pre‑Processing**
   - Read user **Click Events** from Kafka with event-time semantics and watermarks.
   - Read **Order Events** from Kafka for anomaly (flash sale) detection.
   - Read **Inventory Updates** from Kafka.
   - Read **Pricing Rules** (min/max bounds) from Kafka.
   - Fetch **Competitor Prices** from an external REST API.

2. **Demand Aggregation**
   - Calculate a **sliding window** (5-minute window, 1-minute slide) of current demand per product.
   - Calculate a **historical demand average** using a longer window (e.g., 1-hour sliding window).
   - Store both metrics in Flink keyed state for quick lookup.

3. **Anomaly & Flash Sale Detection**
   - Use **Flink CEP** on Order Events to identify flash-sale patterns (e.g., 10 orders/minute spike).
   - Trigger an **emergency price adjustment** (e.g., +20% multiplier) that expires after a configurable time-to-live.

4. **Pricing Calculation**
   - **Baseline Prediction:** Use a machine learning model (TensorFlow) for an initial price. We are building and sharing the TensorFlow model here: https://github.com/kkaravitis/pricing-model
   - **Competitor Adjustment:** Blend competitor prices (e.g., 30% competitor, 70% ML).
   - **Demand Adjustment:** Increase price when current demand exceeds historical average.
   - **Inventory Adjustment:** Increase price when inventory is below a threshold (e.g., <10 units).
   - **Emergency Adjustment:** Apply flash-sale multiplier from CEP output.
   - Enforce **min/max price rules** to ensure legal and business constraints.

5. **Price Broadcasting**
   - Publish updated prices to a Kafka sink with **exactly‑once semantics**.
   - Emit **alerts** (side‑outputs) for significant price changes (e.g., >50% deviation).
---