Pricing Prediction Model — Request for Proposal
1. Background & Goal
   We need a TensorFlow SavedModel that, given realtime product metrics plus the product’s identifier, will output an optimal price recommendation. 
   This model will be invoked from our Flink streaming job via a simple two‐tensor interface (one for numeric features, one for the product ID string).

2. Functional Requirements
   Inputs

product_id: tf.string tensor of shape [batch_size, 1]

numeric_features: tf.float32 tensor of shape [batch_size, F]
where F ≥ 3 (minimum required features:

inventory level

current demand

competitor price)

Output

predicted_price: tf.float32 tensor of shape [batch_size, 1]
(the recommended price for each input row)

SavedModel signature

Name: "serving_default"

Inputs named exactly

"serving_default_product_id" → product_id tensor

"serving_default_input" → numeric_features tensor

Output tensor named "StatefulPartitionedCall" (or please document if different)

3. Model Architecture (suggested)
   Embedding

Map each product_id (string) into a learnable embedding vector of size E (e.g. E = 16).

Concatenation

Concatenate the embedding with the numeric_features vector → total dimension = (E + F).

Feed-forward network

Hidden Layer 1: Dense(64) with ReLU

Hidden Layer 2: Dense(32) with ReLU

Output Layer: Dense(1)

Loss

Mean squared error (MSE) against historical “true” price or revenue‐maximizing price.

Optimizer

Adam with learning rate ~1e-3 (tunable).

4. Training Data
   Features source

Historical inventory levels (snapshot at pricing decision time)

Computed demand metrics

Competitor prices

(Optionally) emergency adjustments, price rules, etc.

Labels

The actual price at which the item sold (or the price that maximized profit in simulation).

Dataset size

Ideally ≥ 100 k examples, covering at least 50 distinct products.

5. Evaluation & Benchmarks
   Primary metric: MSE on a held-out test set.

Secondary metric: Mean absolute percentage error (MAPE).

Target: MSE < X (define based on historical variance), MAPE < 10%.

6. Performance & Scalability
   Must load and run inference in under 10 ms per record in batch mode.

SavedModel directory should be storable as a ZIP (to transport via Kafka bytes).

7. Deliverables
   A zipped TF 2.x SavedModel directory containing:

saved_model.pb

variables/…

Code snippet (Python) showing how to load and run inference:

import tensorflow as tf
model = tf.saved_model.load("/path/to/saved_model")
infer = model.signatures["serving_default"]
logits = infer(
serving_default_product_id=tf.constant([["p-42"]]),
serving_default_input=tf.constant([[ 42.0, 0.5, 10.0 ]])
)["StatefulPartitionedCall"]
Documentation of input/output names, expected dtypes/shapes, and any custom preprocessing.

8. Integration Notes
   We will unzip the model in Flink via Java’s ZipInputStream, then call


SavedModelBundle.load(tmpDir.toString(), "serve");
session.runner()
.feed("serving_default_product_id", idTensor)
.feed("serving_default_input",    featuresTensor)
.fetch("StatefulPartitionedCall")
.run();

Ensure no external dependencies beyond standard TF 2.x and Java string/float tensor support.

