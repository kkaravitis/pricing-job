
# Building Maintainable Streaming Applications with Apache Flink
_A Dynamic Pricing Engine Example Using Hexagonal Architecture and TDD_

## Introduction

Modern streaming applications must process high-volume, low-latency data while remaining maintainable, testable, and adaptable to changing requirements. In this guide, we’ll walk through how to architect, design, and implement a **real-time dynamic pricing engine** with **Apache Flink**, illustrating key software engineering principles:

- **Hexagonal Architecture (Ports & Adapters):** Keep your core business logic decoupled from external systems (Kafka, HTTP, CDC, state backends), making it easy to test and evolve.
- **Test-Driven Development (TDD):** Drive your design with tests, ensuring correctness from day one and supporting safe refactoring as requirements change.
- **Clean Code Practices:** Apply SOLID principles, immutable value objects, and clear separation of concerns to keep your codebase understandable and modular.
- **Streaming Best Practices:** Use Apache Flink’s stateful processing, exactly-once guarantees, sliding windows, and CEP for anomaly detection.

### Who Should Read This

- **Software Architects** looking for patterns to structure large-scale streaming systems.
- **Senior Java Developers** eager to apply clean-code and TDD to data-intensive applications.
- **Data Engineers** seeking to master Flink’s stateful APIs and real-time integration with external services.

### What You’ll Learn

1. **Designing a Clean Core:** Model domain entities (Product, DemandMetrics, PriceRule, etc.) and encapsulate pricing logic in a pure `PricingEngineService`.
2. **Defining Ports & Adapters:** Create interfaces (ports) for all external dependencies—state repos, HTTP clients, ML predictors—and implement adapters for Kafka, Debezium CDC, OkHttp, and Flink state.
3. **Building Stateful Pipelines:** Implement multiple Flink pipelines (“feeders”) to ingest clickstreams, competitor prices, inventory updates, pricing rules, and flash-sale anomalies, each updating its slice of keyed state.
4. **Composing the Pricing Engine:** Combine all state slices in a central Flink pipeline that computes and emits final prices with exactly-once semantics.
5. **Driving Development with TDD:** Write unit tests for your service and adapters, integration tests with Flink’s MiniCluster, and end-to-end tests using Docker Compose.
6. **Operational Best Practices:** Configure Flink checkpoints, manage state TTL, monitor with Prometheus/Grafana, and troubleshoot common streaming issues.

By the end of this guide, you’ll have a blueprint—and reference code—for crafting production-ready, clean-code streaming applications with Apache Flink, using dynamic pricing as a compelling real-world example. Let’s get started!


---


## Part 1: Business Requirements

Before diving into code and architecture, let’s clarify **what** our dynamic pricing engine must achieve from a business perspective.

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
    - Read **Order Events** from Kafka or Debezium CDC for anomaly (flash sale) detection.
    - Read **Inventory Updates** from MySQL via Debezium CDC.
    - Read **Pricing Rules** (min/max bounds) from MySQL via CDC.
    - Fetch **Competitor Prices** from an external REST API.

2. **Demand Aggregation**
    - Calculate a **sliding window** (5-minute window, 1-minute slide) of current demand per product.
    - Calculate a **historical demand average** using a longer window (e.g., 1-hour sliding window).
    - Store both metrics in Flink keyed state for quick lookup.

3. **Anomaly & Flash Sale Detection**
    - Use **Flink CEP** on Order Events to identify flash-sale patterns (e.g., 10 orders/minute spike).
    - Trigger an **emergency price adjustment** (e.g., +20% multiplier) that expires after a configurable time-to-live.

4. **Pricing Calculation**
    - **Baseline Prediction:** Use a machine learning model (TensorFlow) for an initial price.
    - **Competitor Adjustment:** Blend competitor prices (e.g., 30% competitor, 70% ML).
    - **Demand Adjustment:** Increase price when current demand exceeds historical average.
    - **Inventory Adjustment:** Increase price when inventory is below a threshold (e.g., <10 units).
    - **Emergency Adjustment:** Apply flash-sale multiplier from CEP output.
    - Enforce **min/max price rules** to ensure legal and business constraints.

5. **Price Broadcasting & Monitoring**
    - Publish updated prices to a Kafka sink with **exactly‑once semantics**.
    - Emit **alerts** (side‑outputs) for significant price changes (e.g., >50% deviation).
    - Expose key metrics (update latency, throughput, error rates) via Prometheus for Grafana dashboards.

### Non‑Functional Requirements

- **Scalability:** Each pipeline must scale independently (Flink parallelism).
- **Resilience:** Support exactly-once processing and recover from failures via checkpoints and savepoints.
- **Testability:** Core logic must be fully unit-testable; pipelines must support integration tests with Flink MiniCluster.
- **Maintainability:** Clean separation between business logic and infrastructure; easy to add new data sources or pricing rules.

This section lays out the **why** and **what**—the business motivations and concrete requirements—driving our Flink-based dynamic pricing solution.


## Part 2: Hexagonal Architecture (Ports & Adapters)

In a streaming application, coupling business logic directly to infrastructure (Kafka, HTTP, Flink state) makes testing and maintenance difficult. **Hexagonal Architecture**—also known as **Ports & Adapters**—solves this by isolating your core domain logic from external systems.

### Architecture Overview
![enter image description here](file:///C:/Users/kkara/Downloads/architecture.jpg)

- **Core Domain:** Contains pure business logic (`PricingEngineService`, domain entities).
- **Ports:** Interfaces defining required capabilities.
- **Adapters:** Concrete implementations of ports.
- **Tests:** Use mock implementations of ports to unit-test the core domain in isolation.

Hexagonal Architecture is all about **separation of concerns**—keeping your core business logic (the “hexagon”) isolated from technical details (databases, messaging systems, HTTP, etc.). In a streaming context, this brings several crucial benefits:

### 1. Isolation of Business Logic

- **Core Domain** (`PricingEngineService`) only speaks to well-defined interfaces (ports), never directly to Kafka, Flink, HTTP, or databases.
- **Streaming Complexity**—event time, watermarks, state backends, exactly-once semantics—remains in adapters, not in your pricing logic.

**Why it matters:**  
You can write unit tests for your pricing logic without instantiating a Flink environment or mocking Kafka clusters. You verify your business rules purely in memory, with stub implementations of each port.

### 2. Flexible Adapter Swapping

- **Ports** define _what_ your core needs: “Give me the current demand metrics,” “Fetch competitor price.”
- **Adapters** define _how_ that happens: via Flink state, Debezium CDC, Kafka, HTTP calls, or even a database lookup.

**Why it matters:**  
Need to switch from HTTP to Kafka for competitor prices? Just write a new adapter for `CompetitorPriceRepository`—the core doesn’t change. This is critical in streaming, where data sources evolve.

### 3. Clear Testing Boundaries

- **Unit Tests:** Mock ports to simulate demand surges, zero inventory, or flash-sale triggers.
- **Integration Tests:** Wire a real Flink MiniCluster with adapters to test the pipelines end-to-end.
- **End-to-End Tests:** Use Docker Compose (Kafka, MySQL, Flink) with the same adapters to validate production-grade flows.

**Why it matters:**  
Streaming logic often intertwines state management and data ingestion. Hexagonal Architecture lets you validate your core business logic comprehensively before you ever run a Flink job.

### 4. Scalability & Resilience

- **Adapter Scope:** Each adapter is responsible only for connecting to its system (e.g., HTTP client or Flink `ValueState`).
- **Parallelism Control:** You can tune Flink parallelism on a per-pipeline basis without touching your business logic.

**Why it matters:**  
In high-throughput streaming scenarios, the ability to scale individual pipelines (demand, competitor, inventory, rules, flash-sale detection) independently is crucial. Hexagonal Architecture ensures that these scaling changes never ripple into your core domain code.

#### Streaming-Specific Diagram

\`\`\`plaintext
+----------------+    +-----------------+    +----------------+
| Click Events   |    | Inventory CDC   |    | HTTP Competitor|
| Kafka Adapter  |    | Debezium Adapter|    | Adapter        |
+-------+--------+    +--------+--------+    +--------+-------+
|                     |                     |
+----v---------------------v---------------------v----+
|                Flink Pipelines (Feeders)            |
| (each updates its own ValueState via repository)    |
+----+------------+------------+------------+--------+
|            |            |            |
+----v-----+ +----v-----+ +----v-----+ +----v-----+
|Demand    | |Inventory | |Price rules| |Competitor|  ...
|Repository| |Repository| |Repository | |Repository|
+----+-----+ +----+-----+ +----+------+ +----+-----+
|            |            |            |
+------------+------------+------------+
|
+----v----+
|  Ports  |
|(Interfaces)|
+----+----+
|
+-----v-----+
| Core      |
| Pricing   |
| Logic     |
+-----------+
\`\`\`

By applying Hexagonal Architecture to streaming applications, you achieve a **clean separation** between:

1. **What** your business logic needs (ports)
2. **How** data flows in and out in a streaming environment (adapters)

This enables **rapid evolution**, **robust testing**, and **high scalability**—all essential for modern real-time systems.


---


## Part 3: Core Domain Design

The core domain defines the fundamental entities and business logic for our pricing engine. This layer is entirely independent of any external systems, focusing solely on the pricing rules and computations.

### Domain Entities

```java
// Product.java
public record Product(String productId) {}

// ClickEvent.java
public record ClickEvent(String userId, String productId, long timestamp) {}

// CompetitorPrice.java
public record CompetitorPrice(String productId, Money price) {}

// DemandMetrics.java
public record DemandMetrics(String productId, double currentDemand, double historicalAverage) {}

// InventoryEvent.java
public record InventoryEvent(String productId, int level) {}

// PriceRule.java
public record PriceRule(Money minPrice, Money maxPrice) {
    public static PriceRule defaults() {
        return new PriceRule(new Money(BigDecimal.ZERO, "USD"), new Money(BigDecimal.valueOf(Double.MAX_VALUE), "USD"));
    }
}

// PricingContext.java
public record PricingContext(
    Product product,
    DemandMetrics demandMetrics,
    int inventoryLevel,
    CompetitorPrice competitorPrice,
    PriceRule priceRule,
    boolean flashSale
) {}

// PricingResult.java
public record PricingResult(Product product, Money newPrice) {}
```

These immutable records represent the core data structures your pricing logic will consume.

### The Money Value Object

```java
public final class Money implements Serializable {
    private final BigDecimal amount;
    private final String currency;

    public Money(BigDecimal amount, String currency) {
        this.amount = amount;
        this.currency = currency;
    }

    public Money multiply(double factor) {
        return new Money(amount.multiply(BigDecimal.valueOf(factor)), currency);
    }

    public Money add(Money other) {
        if (!currency.equals(other.currency)) {
            throw new IllegalArgumentException("Currency mismatch");
        }
        return new Money(amount.add(other.amount), currency);
    }

    public boolean isLessThan(Money other) {
        return amount.compareTo(other.amount) < 0;
    }

    public boolean isGreaterThan(Money other) {
        return amount.compareTo(other.amount) > 0;
    }

    @Override
    public String toString() {
        return currency + " " + amount;
    }
}
```

**Money** is a robust value object for monetary calculations, ensuring currency safety and immutability.

### PricingEngineService

```java
public class PricingEngineService {
    private final DemandMetricsRepository demandRepo;
    private final InventoryLevelRepository invRepo;
    private final CompetitorPriceRepository compRepo;
    private final PriceRuleRepository ruleRepo;
    private final ModelInferencePricePredictor mlPredictor;
    private final EmergencyPriceAdjustmentRepository emergencyRepo;

    public PricingEngineService(
        DemandMetricsRepository demandRepo,
        InventoryLevelRepository invRepo,
        CompetitorPriceRepository compRepo,
        PriceRuleRepository ruleRepo,
        ModelInferencePricePredictor mlPredictor,
        EmergencyPriceAdjustmentRepository emergencyRepo
    ) {
        this.demandRepo = demandRepo;
        this.invRepo = invRepo;
        this.compRepo = compRepo;
        this.ruleRepo = ruleRepo;
        this.mlPredictor = mlPredictor;
        this.emergencyRepo = emergencyRepo;
    }

    public PricingResult computePrice(String productId) throws PricingException {
        // 1. Fetch Data
        DemandMetrics dm = demandRepo.getDemandMetrics(productId);
        int inventory = invRepo.getInventoryLevel(productId);
        CompetitorPrice cp = compRepo.getCompetitorPrice(productId);
        PriceRule rule = ruleRepo.getPriceRule(productId);
        boolean flashSale = emergencyRepo.getAdjustmentFactor(productId) > 1.0;

        // 2. Baseline via ML
        PricingContext ctx = new PricingContext(
            new Product(productId), dm, inventory, cp, rule, flashSale
        );
        Money price = mlPredictor.predictPrice(ctx);

        // 3. Competitor Adjustment
        price = price.multiply(0.7).add(cp.price().multiply(0.3));

        // 4. Demand Adjustment
        if (dm.currentDemand() > dm.historicalAverage()) {
            price = price.multiply(1.05);
        }

        // 5. Inventory Adjustment
        if (inventory < 10) {
            price = price.multiply(1.10);
        }

        // 6. Emergency Adjustment
        double factor = emergencyRepo.getAdjustmentFactor(productId);
        if (factor > 1.0) {
            price = price.multiply(factor);
        }

        // 7. Enforce Price Rules
        if (price.isLessThan(rule.minPrice())) price = rule.minPrice();
        if (price.isGreaterThan(rule.maxPrice())) price = rule.maxPrice();

        return new PricingResult(new Product(productId), price);
    }
}
```

The **PricingEngineService** encapsulates all pricing logic in a pure, side-effect-free class, operating only on domain types and ports. This makes it fully testable and independent of any streaming framework or external system.
The `computePrice` method begins by fetching all relevant data for a product—demand metrics (current and historical) via `demandRepo`, inventory level via `invRepo`, competitor price via `compRepo`, pricing rules via `ruleRepo`, and an emergency multiplier via `emergencyRepo`. It then builds a `PricingContext` and calls the ML predictor to obtain a baseline price. Next, it adjusts that baseline by blending in competitor prices (e.g., 30% weight), increasing the price if current demand exceeds the historical average, and applying a markup when inventory is low. If an emergency factor is present (flash-sale), it multiplies the price accordingly. Finally, it clamps the resulting price within the business-defined minimum and maximum bounds before returning the `PricingResult`.


---

## Part 4: Ports & Adapters

In Hexagonal Architecture, **Ports** define the required operations, and **Adapters** provide concrete implementations. Here are the key ports and their adapters:

### Ports

```java
public interface CompetitorPriceRepository {
    CompetitorPrice getCompetitorPrice(String productId) throws PricingException;
}

public interface DemandMetricsRepository {
    DemandMetrics getDemandMetrics(String productId) throws PricingException;
}

public interface InventoryLevelRepository {
    int getInventoryLevel(String productId) throws PricingException;
}

public interface PriceRuleRepository {
    PriceRule getPriceRule(String productId) throws PricingException;
}

public interface EmergencyPriceAdjustmentRepository {
    double getAdjustmentFactor(String productId) throws PricingException;
}

public interface ModelInferencePricePredictor {
    Money predictPrice(PricingContext context) throws PricingException;
}
```

### HTTP Adapter (Competitor Prices)

```java
package com.wordpress.kkaravitis.pricing.adapters.competitor;

public class HttpCompetitorPriceRepository implements CompetitorPriceRepository {
    private final HttpServiceClient client;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpCompetitorPriceRepository(HttpServiceClient client, String baseUrl) {
        this.client = client;
        this.baseUrl = baseUrl;
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) throws PricingException {
        try {
            String url = String.format("%s/price/%s", baseUrl, productId);
            String json = client.get(url);
            if (json == null) {
                return new CompetitorPrice(productId, new Money(0.0, "USD"));
            }
            JsonNode node = mapper.readTree(json);
            double price = node.get("price").asDouble();
            return new CompetitorPrice(productId, new Money(price, "USD"));
        } catch (Exception e) {
            throw new PricingException("Failed to fetch competitor price for " + productId, e);
        }
    }
}
```
The `HttpCompetitorPriceRepository` adapter implements the `CompetitorPriceRepository` port by making HTTP GET calls to an external REST endpoint to fetch the latest competitor price for a given product. It uses an `HttpServiceClient` abstraction to handle the request and parses the JSON response into a `CompetitorPrice` domain object. If the service returns no data or a 404, it gracefully falls back to a default price of 0.0 USD.

*HttpCompetitorPriceRepository* uses an `HttpServiceClient` to fetch competitor prices via REST, parsing JSON responses into domain objects.

### Flink State Repositories

#### FlinkCompetitorPriceRepository

```java
package com.wordpress.kkaravitis.pricing.adapters.competitor;

public class FlinkCompetitorPriceRepository implements CompetitorPriceRepository, Serializable {
    private transient ValueState<CompetitorPrice> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<CompetitorPrice> desc =
            new ValueStateDescriptor<>("competitor-price", Types.POJO(CompetitorPrice.class));
        this.state = ctx.getState(desc);
    }

    public void updatePrice(CompetitorPrice price) throws PricingException {
        state.update(price);
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) throws PricingException {
        CompetitorPrice cp = state.value();
        return cp != null ? cp : new CompetitorPrice(productId, new Money(0.0, "USD"));
    }
}
```
`FlinkCompetitorPriceRepository` uses Flink’s keyed `ValueState<CompetitorPrice>` to store and retrieve the most recent competitor price per product ID. The `updatePrice` method is called by the competitor price feeder pipeline to persist the latest price, and `getCompetitorPrice` reads from state, returning a default of 0.0 USD if no value is present. This ensures the pricing engine always has access to up-to-date competitor data.
#### FlinkDemandMetricsRepository

```java
package com.wordpress.kkaravitis.pricing.adapters;

public class FlinkDemandMetricsRepository implements DemandMetricsRepository, Serializable {
    private transient ValueState<DemandMetrics> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<DemandMetrics> desc =
            new ValueStateDescriptor<>("demand-metrics", Types.POJO(DemandMetrics.class));
        this.state = ctx.getState(desc);
    }

    public void updateMetrics(DemandMetrics metrics) throws PricingException {
        state.update(metrics);
    }

    @Override
    public DemandMetrics getDemandMetrics(String productId) throws PricingException {
        DemandMetrics metrics = state.value();
        return metrics != null ? metrics : new DemandMetrics(productId, 0.0, 0.0);
    }
}
```
`FlinkDemandMetricsRepository` maintains sliding-window demand metrics (current and historical averages) in a keyed `ValueState<DemandMetrics>`. The demand metrics pipeline updates this state as clickstream data flows through, and the core logic retrieves both metrics via `getDemandMetrics`. If no metrics are present, it defaults to zero demand, ensuring stable calculations.
#### FlinkInventoryLevelRepository

```java
package com.wordpress.kkaravitis.pricing.adapters;

public class FlinkInventoryLevelRepository implements InventoryLevelRepository, Serializable {
    private transient ValueState<Integer> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<Integer> desc =
            new ValueStateDescriptor<>("inventory-quantity", Types.INT);
        this.state = ctx.getState(desc);
    }

    public void updateLevel(int level) throws PricingException {
        state.update(level);
    }

    @Override
    public int getInventoryLevel(String productId) throws PricingException {
        Integer level = state.value();
        return level != null ? level : 0;
    }
}
```
`FlinkInventoryLevelRepository` tracks real-time inventory levels per product using `ValueState<Integer>`. Inventory updates from a Debezium CDC pipeline invoke `updateLevel`, writing the latest stock count to state. The `getInventoryLevel` method then provides the current level to the pricing engine, defaulting to zero if not yet initialized.
#### FlinkPriceRuleRepository

```java
package com.wordpress.kkaravitis.pricing.adapters;

public class FlinkPriceRuleRepository implements PriceRuleRepository, Serializable {
    private transient ValueState<PriceRule> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<PriceRule> desc =
            new ValueStateDescriptor<>("price-rule", PriceRule.class);
        this.state = ctx.getState(desc);
    }

    public void updateRule(PriceRule rule) throws PricingException {
        state.update(rule);
    }

    @Override
    public PriceRule getPriceRule(String productId) throws PricingException {
        PriceRule rule = state.value();
        return rule != null ? rule : PriceRule.defaults();
    }
}
```
`FlinkPriceRuleRepository` stores business-defined minimum and maximum price rules in `ValueState<PriceRule>`. Whenever pricing rules change in the upstream database (via CDC), the rules pipeline calls `updateRule` to write the new bounds. The pricing engine retrieves these via `getPriceRule`, using default unbounded rules if none are set, ensuring legal and business constraints are always enforced.
#### FlinkEmergencyAdjustmentRepository

```java
package com.wordpress.kkaravitis.pricing.adapters;

public class FlinkEmergencyAdjustmentRepository implements EmergencyPriceAdjustmentRepository, Serializable {
    private transient ValueState<Double> state;

    public void initializeState(RuntimeContext ctx) {
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Duration.ofMinutes(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();
        ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("emergency-adjustment", Double.class);
        desc.enableTimeToLive(ttlConfig);
        this.state = ctx.getState(desc);
    }

    public void updateAdjustment(double factor) throws PricingException {
        state.update(factor);
    }

    @Override
    public double getAdjustmentFactor(String productId) throws PricingException {
        Double factor = state.value();
        return factor != null ? factor : 1.0;
    }
}
```
`FlinkEmergencyAdjustmentRepository` applies emergency (flash-sale) multipliers with a TTL. It uses a keyed `ValueState<Double>` configured with `StateTtlConfig` so that adjustment factors automatically expire after a set duration (e.g., 10 minutes). The CEP pipeline detects anomalies and calls `updateAdjustment`, while `getAdjustmentFactor` returns either the current factor or 1.0 when no emergency is active.
### Machine Learning Adapter (MlModelAdapter)

```java
package com.wordpress.kkaravitis.pricing.adapters.ml;

@NoArgsConstructor
public class MlModelAdapter implements ModelInferencePricePredictor, Serializable {
    private static final long serialVersionUID = 1L;

    private transient byte[] modelBytes;
    private transient TransformedModel model;
    private transient ModelDeserializer modelDeserializer;

    public void initialize() {
        this.modelDeserializer = new ModelDeserializer();
    }

    public void updateModelBytes(byte[] bytes) {
        this.modelBytes = bytes;
        this.model = null;
    }

    @Override
    public Money predictPrice(PricingContext context) {
        if (model == null) {
            if (modelBytes == null) {
                throw new IllegalStateException("Model bytes not initialized");
            }
            model = modelDeserializer.deserialize(modelBytes);
        }
        return model.predict(context);
    }
}
```
The `MlModelAdapter` implements the `ModelInferencePricePredictor` port and handles dynamic loading of a TensorFlow model from raw bytes. It holds the model bytes and a `TransformedModel` instance, lazily deserializing on first use via `ModelDeserializer`. Calling `predictPrice` runs the deserialized model against a `PricingContext`, returning a `Money` prediction. This allows live model updates without restarting the Flink job.

