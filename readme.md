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

## Architecture Overview

### 1. Multi‑Pipeline Dataflow in a Single Flink Job

We partition our Flink job into **independent feeder pipelines**—each responsible for ingesting one source of truth and updating a dedicated state store—plus a **final pricing pipeline** that reads all state and computes prices.

```text
  [ClickEvents] ───► [DemandMetricsPipeline] ─┐
                                              │
  [ClickEvents] ───► [CompetitorPipeline] ────┼─► [PricingPipeline] ─► KafkaSink
                                              │
  [InventoryCDC] ─► [InventoryPipeline] ──────┼─► (state providers read here)
                                              │
  [RulesCDC] ─────► [PriceRulePipeline] ──────┼
                                              │
  [ModelKafka] ───► [ModelBroadcastPipeline] ─┘           
```  

- **DemandMetricsPipeline**
   - **Source:** Kafka clickstream
   - **Transform:** Sliding windows (5 min length, 1 min slide) → `DemandMetrics`
   - **State:** `MapState<String, DemandMetrics>`
  

- **InventoryPipeline**
   - **Source:** Debezium CDC on warehouse DB
   - **State:** `ValueState<String, Integer>` (inventory level)


- **CompetitorPipeline**
   - **Source:** Click IDs → Async HTTP lookup
   - **State:** `ValueState<String, CompetitorPrice>`



- **PriceRulePipeline**
   - **Source:** Debezium CDC on pricing rules table
   - **State:** `ValueState<String, PriceRule>`



- **ModelBroadcastPipeline**
   - **Source:** Kafka `model-topic`
   - **Broadcast:** serialized ML model bytes to every parallel task



- **PricingPipeline**
   - **Source:** Kafka clickstream (keyed by `productId`)
   - **Connect:** Broadcast model bytes
   - **Process:** `PricingWithModelBroadcastFunction`
      - Reads all keyed state adapters + model adapter
      - Invokes `PricingEngineService`
      - Emits new prices to exactly‑once Kafka sink  


### 2. Hexagonal Code Organization

Our code follows the **ports‑and‑adapters** pattern:

```text
                ┌───────────────────────┐
                │   Domain Layer        │
                │  (pure business)      │
                ├───────────────────────┤
                │ PricingEngineService  │
                │ PricingContext        │
                │ Money, PriceRule      │
                │ Port Interfaces:      │
                │  • DemandMetrics      │
                │  • Inventory          │
                │  • CompetitorPrice    │
                │  • PriceRule          │
                │  • ModelInference     │
                └───────────────────────┘
                           ▲
                           │
             Ports &       │
             Adapters      ▼
 ┌─────────────────────────────────────────┐
 │ Infrastructure Layer (Flink Adapters &  │
 │ Builders)                               │
 │ ─────────────────────────────────────── │
 │  • DataflowBuilder classes (one per     │
 │    pipeline)                            │
 │  • FlinkXxxRepository (stateful         │
 │    adapters)                            │
 │  • HttpCompetitorPriceRepository        │
 │  • Kafka*, CDC Sources                  │
 │  • PricingWithModelBroadcastFunction    │
 │  • KafkaSink for output                 │
 └─────────────────────────────────────────┘
```

In our dynamic pricing application, the **heart of the business logic** lives in the `PricingEngineService`. By organizing code around **ports** (interfaces) and **adapters** (concrete implementations), we achieve:

- **Separation of concerns:** business rules vs. infrastructure
- **Testability:** domain code can be exercised in isolation
- **Flexibility:** swap adapters (e.g. for HTTP vs. DB) without touching core logic    

## Hexagonal Architecture Deep Dive

In our dynamic pricing application, the **heart of the business logic** lives in the `PricingEngineService`. By organizing code around **ports** (interfaces) and **adapters** (concrete implementations), we achieve:

- **Separation of concerns:** business rules vs. infrastructure
- **Testability:** domain code can be exercised in isolation
- **Flexibility:** swap adapters (e.g. for HTTP vs. DB) without touching core logic

---

### 1. Core Domain & Ports

```text
┌───────────────────────────────────────────────────────────────────────────────────┐
│                     Domain Layer                                                  │
│  (Pure business logic, no Flink or I/O dependencies)                              │
├───────────────────────────────────────────────────────────────────────────────────┤
│ class PricingEngineService {                                                      │
│   // Constructor injects ports (abstractions)                                     │
│   PricingEngineService(                                                           │
│     DemandMetricsRepository demandMetricsRepository,                              │
│     InventoryRepository inventoryRepository,                                      │
│     CompetitorPriceRepository competitorPriceRepository,                          │
│     PriceRuleRepository priceRuleRepository,                                      │
│     ModelInferencePricePredictor modelInferencePricePredictor                     │
│   ) { … }                                                                         │
│                                                                                   │
│   // Business method:!                                                            │
│   PricingResult computePrice(String productId) {                                  │
│     // 1) Fetch per-product data via ports                                        │
│     DemandMetrics dm = demandMetricsRepository.getDemandMetrics(productId);       │
│     int invLevel = inventoryRepository.getInventoryLevel(productId);              │
│     CompetitorPrice cp = competitorPriceRepository.getCompetitorPrice(productId); │
│     PriceRule rule = rulePort.getPriceRule(productId);                            │
│                                                                                   │
│     // 2) ML base price                                                           │
│     Money mlPrice = modelInferencePricePredictor.predictPrice(                    │
│        new PricingContext(productId, dm, invLevel, cp, rule)                      │
│     );                                                                            │
│                                                                                   │
│     // 3) Blend & adjust                                                          │
│     Money finalPrice = applyBusinessRules(dm, invLevel, cp,                       │
│                        mlPrice, rule);                                            │
│                                                                                   │
│     return new PricingResult(productId, finalPrice);                              │
│   }                                                                               │
└───────────────────────────────────────────────────────────────────────────────────┘
```
### Key Points

- **Ports** are simple Java interfaces in `com.mycompany.pricing.domain.port`:
    - `DemandMetricsRepository`
    - `InventoryRepository`
    - `CompetitorPriceRepository`
    - `PriceRuleRepository`
    - `ModelInferencePricePredictor`

- The `PricingEngineService` depends **only** on these interfaces (no Flink, no HTTP, no Kafka).

### Adapters & DataflowBuilders

Each port has one or more adapters in the infrastructure layer under `com.mycompany.pricing.infrastructure`:

| **Port Interface**                  | **Flink Adapter**                | **Description**                                   |
|-------------------------------------|----------------------------------|---------------------------------------------------|
| `DemandMetricsRepository`           | `FlinkDemandMetricsRepository`   | Window & state logic feeds `MapState`             |
| `InventoryRepository`               | `FlinkInventoryRepository`       | CDC → `ValueState<Integer>`                       |
| `CompetitorPriceRepository`         | `FlinkCompetitorPriceRepository` | Async HTTP → `ValueState<CompetitorPrice>`        |
| `PriceRuleRepository`               | `FlinkPriceRuleRepository`       | CDC → `ValueState<PriceRule>`                     |
| `ModelInferencePricePredictor`      | `MlModelAdapter`                 | Broadcast model bytes → `predictPrice()`          |

Each adapter pipeline lives in its own `*PipelineBuilder` class. Below is how we structure and wire each flow in the infrastructure layer:

#### Pipeline #1: Demand Metrics Aggregation
The **DemandMetricsPipelineBuilder** reads click events from Kafka, assigns event‑time watermarks, computes both short‑term (5 min) and long‑term (1 h) demand metrics with sliding windows, and writes the results into a keyed `MapState`. The Flink adapter `FlinkDemandMetricsRepository` then serves those metrics via the `DemandMetricsRepository` port.
```text
+----------------------+
|   ClickEvent Stream  |
+----------+-----------+
           |
           ▼
+------------------------------+
| AssignTimestampsAndWatermarks|
+----------+-------------------+
           |
           ▼
+-----------------------------------+
| SlidingWindow(5m length,1m slide) |
+----------+------------------------+
           |
           ▼
+-----------------------------+
| ProcessWindowFunction       |
| → DemandMetrics             |
+----------+------------------+
           |
           ▼
+-----------------------------+
| KeyBy productId             |
+----------+------------------+
           |
           ▼
+--------------------------------+
| UpdateDemandState              |
| (KeyedProcessFunction →        |
|  FlinkDemandMetricsRepository) |
+--------------------------------+
```

#### Pipeline #2: Inventory Level Updates
The **InventoryPipelineBuilder** ingests CDC events from the warehouse database, parses each `InventoryEvent` POJO, and updates a keyed `ValueState<Integer>` in `FlinkInventoryRepository`. This repository implements the `InventoryRepository` port.
```text
+--------------------------+
|   Inventory CDC Stream   |
+----------+---------------+
           |
           ▼
+-----------------------------+
| ParseInventoryEvent (POJO)  |
+----------+------------------+
           |
           ▼
+----------------------+  
| KeyBy productId      |
+----------+-----------+
           |
           ▼
+-----------------------------+
| UpdateInventoryState        |
| (KeyedProcessFunction →     |
|  FlinkInventoryRepository)  |
+-----------------------------+
```

#### Pipeline #3: Competitor Price Enrichment
The **CompetitorPipelineBuilder** extracts product IDs from click events, performs an asynchronous HTTP lookup to fetch competitor prices, and stores the results in a keyed `ValueState<CompetitorPrice>` via `FlinkCompetitorPriceRepository`, fulfilling the `CompetitorPriceRepository` port.

```text
+----------------------+
|   ClickEvent Stream  |
+----------+-----------+
           |
           ▼
+----------------------+
| ExtractProductId     |
+----------+-----------+
           |
           ▼
+------------------------------+
| AsyncCompetitorEnrichment    |
+----------+-------------------+
           |
           ▼
+----------------------+  
| KeyBy productId      |
+----------+-----------+
           |
           ▼
+-----------------------------+
| UpdateCompetitorState       |
| (KeyedProcessFunction →     |
|  FlinkCompetitorRepository) |
+-----------------------------+
```

#### Pipeline #4: Pricing Rules CDC

The **PriceRulePipelineBuilder** listens to the pricing rules table via Debezium CDC, converts each change event into a `PriceRuleUpdate` POJO, and updates a keyed `ValueState<PriceRule>` in `FlinkPriceRuleRepository`, our implementation of the `PriceRuleRepository` port.

```text
+-------------------------+
|   PriceRules CDC Stream |
+----------+--------------+
           |
           ▼
+-----------------------------+
| ParseRuleUpdate (POJO)      |
+----------+------------------+
           |
           ▼
+----------------------+  
| KeyBy productId      |
+----------+-----------+
           |
           ▼
+-----------------------------+
| UpdateRuleState             |
| (KeyedProcessFunction →     |
|  FlinkPriceRuleRepository)  |
+-----------------------------+

```

#### Pipeline #5: ML Model Broadcast

The **ModelBroadcastPipelineBuilder** reads serialized ML model bytes from Kafka, broadcasts them to all parallel task instances, and pushes them into the `BroadcastModelInferencePort` adapter which implements `ModelInferencePricePredictor`.

```text
+-----------------------------+
|    Model Kafka Stream       |
+----------+------------------+
           |
           ▼
+-----------------------------+
| Broadcast model bytes       |
| (BroadcastStream<byte[]>)   |
+----------+------------------+
           |
           ▼
+-------------------------------+
| Load into ModelAdapter        |
| (BroadcastModelInferencePort) |
+-------------------------------+
```

#### Main Pricing Pipeline

Once all feeder pipelines are running and state is populated, the **Main Pricing Pipeline** ties everything together:

```text
+----------------------+
|   ClickEvent Stream  |
+----------+-----------+
           |
           ▼
+-----------------------------+
| KeyBy productId             |
+----------+------------------+
           |
           ▼
+----------------------+------+      +-----------------------------+
| .connect(modelBroadcast)    |─────▶| BroadcastModelInferencePort |
+-----------------------------+      +-----------------------------+
           |
           ▼
+---------------------------------------------------------------+
| PricingWithModelBroadcastFunction                             |
|  • open(): initialize all repositories and model adapter      |
|  • processBroadcastElement(bytes):                            |
|      – modelAdapter.updateModelBytes(bytes)                   |
|  • processElement(click):                                     |
|      1. rule   = ruleRepo.getPriceRule(click.productId)       |
|      2. demand = demandRepo.getDemandMetrics(click.productId) |
|      3. inv    = invRepo.getInventoryLevel(click.productId)   |
|      4. comp   = compRepo.getCompetitorPrice(click.productId) |
|      5. mlBase = modelAdapter.predictPrice(context)           |
|      6. result = pricingService.computePrice(click.productId) |
|      7. out.collect(result)                                   |
+---------------------------------------------------------------+
           |
           ▼
+------------------------------+
| KafkaSink (exactly‑once)     |
+----------------------------- +

```

**Detailed Steps:**

1. **Keying**
    - The click event stream is partitioned by `productId`, so all events and state for a given product land on the same task.

2. **Broadcast Connect**
    - We connect the keyed click stream to the model broadcast stream (of byte[]), delivering new model versions to every task.

3. **Function Initialization (`open`)**
    - Inside `PricingWithModelBroadcastFunction.open()`, we call `initializeState()` on:
        - `FlinkPriceRuleRepository`
        - `FlinkDemandMetricsRepository`
        - `FlinkInventoryRepository`
        - `FlinkCompetitorPriceRepository`
        - `ModelInferencePricePredictor`

4. **Model Updates (`processBroadcastElement`)**
    - Whenever a new ML model byte[] arrives, we update the in‑task `modelAdapter` via `.updateModelBytes()`.
    - This avoids checkpointing large models in Flink state.

5. **Pricing Logic (`processElement`)**
    - For each click:
        1. **Fetch** latest per‑product data from each repository port.
        2. **Predict** ML base price: `modelAdapter.predictPrice(context)`.
        3. **Compute final price**: `pricingService.computePrice(productId)`, which applies blending, demand/inventory adjustments, and clamps within the `PriceRule`.
        4. **Emit** a `PricingResult` to the exactly‑once Kafka sink.

6. **Sink**
    - The final prices are written to a Kafka topic using Flink’s two‑phase commit, ensuring end‑to‑end exactly‑once semantics.

This pipeline is purely a **reader** of state and a **caller** of the domain service. All event parsing, windowing, CDC integration, and async enrichment have already been handled by the independent feeder pipelines.