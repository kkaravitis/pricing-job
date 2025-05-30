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
import com.wordpress.kkaravitis.pricing.domain.PricingRuntimeException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.SessionFunction;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TString;

public class ModelDeserializer {

    public TransformedModel deserialize(byte[] bytes) {
        try {
            // 1. Create temp dir and unzip model
            Path tmpDir = Files.createTempDirectory("tf-model-");

            try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(bytes))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    Path outPath = tmpDir.resolve(entry.getName()).normalize();

                    // Prevent Zip Slip vulnerability
                    if (!outPath.startsWith(tmpDir)) {
                        throw new IOException("Bad zip entry: " + entry.getName());
                    }

                    if (entry.isDirectory()) {
                        Files.createDirectories(outPath);
                    } else {
                        Files.createDirectories(outPath.getParent());
                        Files.copy(zis, outPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                    zis.closeEntry();
                }
            }

            // 2. Find actual model directory (in case zip contains subfolder)
            Path modelPath = findModelRoot(tmpDir);

            // 3. Load model
            SavedModelBundle bundle = SavedModelBundle.load(modelPath.toString(), "serve");
            SessionFunction serving = bundle.function("serving_default");

            // 4. Return callable model
            return ctx -> {
                float[] features = new float[]{
                      ctx.inventoryLevel(),
                      (float) ctx.demandMetrics().currentDemand(),
                      (float) ctx.competitorPrice().price().getAmount().doubleValue()
                };

                try (
                      TFloat32 featureTensor = TFloat32.tensorOf(Shape.of(1, 3), data -> {
                          for (int i = 0; i < features.length; i++) {
                              data.setFloat(features[i], 0, i);
                          }
                      });
                      TString productIdTensor = TString.tensorOf(
                            NdArrays.ofObjects(String.class, Shape.of(1, 1))
                                  .setObject(ctx.product().productId(), 0, 0)
                      )
                ) {
                    try (var result = serving.call(Map.of(
                          "serving_default_product_id", productIdTensor,
                          "serving_default_input", featureTensor
                    ))) {

                        try (Tensor output = result.get("Identity").orElseThrow()) {
                            float prediction = output.asRawTensor().data().asFloats().getFloat(0);
                            return new Money(BigDecimal.valueOf(prediction), ctx.priceRule().minPrice().getCurrency());
                        }
                    }
                }
            };

        } catch (Exception e) {
            throw new PricingRuntimeException("Failed to load TensorFlow model", e);
        }
    }

    private Path findModelRoot(Path baseDir) throws IOException {
        // Look for a directory containing saved_model.pb and variables/
        try (var paths = Files.walk(baseDir, 2)) {
            return paths
                  .filter(Files::isDirectory)
                  .filter(p -> Files.exists(p.resolve("saved_model.pb")) && Files.exists(p.resolve("variables")))
                  .findFirst()
                  .orElseThrow(() -> new IOException("No valid TensorFlow SavedModel found"));
        }
    }
}
