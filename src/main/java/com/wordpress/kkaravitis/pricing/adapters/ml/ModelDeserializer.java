package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingRuntimeException;
import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

/**
 * Utility to convert raw model bytes into a TransformedModel instance.
 * Here we assume the model bytes are a zip of a TensorFlow SavedModel directory.
 */
public class ModelDeserializer {

    private ModelDeserializer() {
    }

    public static TransformedModel deserialize(byte[] bytes) {
        try {
            // 1) Write bytes to a temp file and unzip
            Path tmpDir = Files.createTempDirectory("tf-model-");
            Path zipPath = tmpDir.resolve("model.zip");
            Files.write(zipPath, bytes, StandardOpenOption.CREATE);
            try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(zipPath))) {
                ZipEntry entry;
                while ((entry = zin.getNextEntry()) != null) {
                    Path out = tmpDir.resolve(entry.getName());
                    if (entry.isDirectory()) {
                        Files.createDirectories(out);
                    } else {
                        Files.createDirectories(out.getParent());
                        Files.copy(zin, out, StandardCopyOption.REPLACE_EXISTING);
                    }
                    zin.closeEntry();
                }
            }

            // 2) Load the SavedModel
            SavedModelBundle bundle = SavedModelBundle.load(tmpDir.toString(), "serve");
            Session session = bundle.session();

            // 3) Return a TransformedModel wrapping the TF session
            return ctx -> {
                // Build input tensor from your context features
                float[] features = new float[] {
                      ctx.getInventoryLevel(),
                      (float) ctx.getDemandMetrics().getCurrentDemand(),
                      (float) ctx.getCompetitorPrice().getPrice().getAmount().doubleValue()
                };
                try (Tensor<Float> input = Tensor.create(new long[]{1, features.length}, FloatBuffer.wrap(features));
                      Tensor<Float> output = session.runner()
                            .feed("serving_default_input", input)
                            .fetch("StatefulPartitionedCall")
                            .run().get(0).expect(Float.class)) {
                    float[][] outVal = new float[1][1];
                    output.copyTo(outVal);
                    return new Money(outVal[0][0], ctx.getPriceRule().getMinPrice().getCurrency());
                }
            };
        } catch (IOException e) {
            throw new PricingRuntimeException("Failed to load TensorFlow model", e);
        }
    }
}