# Dynamic Pricing Flink Application

A **Dynamic Pricing Engine** continuously adjusts product prices in real time based on evolving business signals—such as live customer demand, inventory levels, competitor pricing, and machine‑learning model predictions. By reacting to market dynamics in sub‑second latency, retailers and marketplaces can optimize revenue and inventory turnover while maintaining competitive positioning.

This implementation uses **Apache Flink 1.18** to deliver a scalable, fault‑tolerant pricing service with these key capabilities:

- **Multiple independent dataflows** feeding state (CDC, async HTTP, windowed aggregation).
- **Keyed state** adapters for per-product data (inventory, demand metrics, rules, competitor prices).
- **Broadcast state** for a single global ML model.
- **Hexagonal design**: domain services depend only on port interfaces and value objects.
- **Loose coupling**: separate `DataflowBuilder` classes initialize each state feed.
- **Main pricing pipeline**: consumes click events, connects to the ML model broadcast, and delegates to domain service for price computation.

## Initial Requirements

Based on the business goals and Flink features outlined below, this application must:

1. **Ingest & Pre‑process**
    - Read **Clickstream** events from Kafka (JSON/Avro) with event‑time timestamps & watermarks.
    - Read **Order** events via KafkaSource or Debezium CDC for anomaly detection.
    - Read **Inventory Updates** via Flink CDC (Debezium) on the warehouse DB.
    - Read **Pricing Rules** (human‑defined min/max bounds) and **ML Models** (serialized PMML/TensorFlow) from Kafka, to be broadcast.

2. **Demand Aggregation**
    - Key click events by `productId` and compute sliding windows (5 min length, 1 min slide) for current demand rate.
    - Compute a longer historical demand average (e.g. 1 h sliding window) and store both metrics in keyed state.

3. **Anomaly/Pattern Detection**
    - Feed **Order** events into Flink CEP to detect flash‑sale or spike patterns requiring emergency price adjustments.

4. **Broadcast Rules & Models**
    - Broadcast **Pricing Rules** and **Global ML Model** updates as side‑inputs so every parallel task sees the latest.
    - Avoid large broadcast maps for rules by using keyed state or external lookups when rule sets grow large.

5. **Price Computation**
    - In a **KeyedBroadcastProcessFunction**, combine:
        - Demand metrics (MapState)
        - Current inventory (ValueState)
        - Competitor prices (async HTTP + ValueState)
        - Broadcast rules & ML inference
    - Trigger on a processing‑time timer (e.g. every minute) to recalculate / emit prices even if no click occurs.

6. **Sink New Prices**
    - Send updated prices to Kafka sink with exactly‑once semantics (two‑phase commit).
    - Emit side‑outputs for alerts (e.g. >50% price change).

---