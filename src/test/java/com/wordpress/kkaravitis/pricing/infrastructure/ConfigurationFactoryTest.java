package com.wordpress.kkaravitis.pricing.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.wordpress.kkaravitis.pricing.infrastructure.config.ConfigurationFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigurationFactoryTest {

    private final ConfigurationFactory factory = new ConfigurationFactory();

    @Test
    void testFlattenNestedMap() {
        Map<String, Object> nested = new HashMap<>();
        nested.put("simple", "foo");
        Map<String, Object> level1 = new HashMap<>();
        level1.put("inner", 42);
        Map<String, Object> level2 = new HashMap<>();
        level2.put("deep", true);
        level1.put("nestedMap", level2);
        nested.put("level1", level1);

        Map<String, String> flat = new HashMap<>();
        // use reflection to call private flatten
        ConfigurationFactory
              .class
              .getDeclaredMethods()[0] // flatten is the only private method
              .trySetAccessible();
        ConfigurationFactoryTestHelper.flatten(nested, flat);

        assertEquals("foo", flat.get("simple"));
        assertEquals("42", flat.get("level1.inner"));
        assertEquals("true", flat.get("level1.nestedMap.deep"));
        // no extra keys
        assertEquals(3, flat.size());
    }

    @Test
    void testBuildWithExternalConfig(@TempDir Path tmp) throws IOException {
        // 1) write a small config.yaml
        String yaml =
              """
                    foo:
                      bar: "baz"
                    number: 12345
                    """;
        System.out.println(yaml);
        Path cfgFile = tmp.resolve("config.yaml");
        Files.writeString(cfgFile, yaml);

        // 2) build with configLocation pointing to tmp
        ParameterTool params = ParameterTool.fromArgs(new String[]{
              "--configLocation", tmp.toString()
        });
        Configuration conf = factory.build(params);

        // 3) verify flattened entries

        assertEquals("baz", conf.get(ConfigOptions.key("foo.bar")
              .stringType().noDefaultValue()));
        assertEquals(12345, conf.get(ConfigOptions.key("number")
              .intType().noDefaultValue()));

        // also test default on missing key
        assertNull(conf.get(ConfigOptions.key("does.not.exist").stringType().noDefaultValue()));
    }


    @Test
    void testBuildInvalidConfigLocation() {
        // non-existent folder
        ParameterTool params = ParameterTool.fromArgs(new String[]{
              "--configLocation", "/path/does/not/exist"
        });
        assertThrows(IOException.class, () -> factory.build(params));
    }

    /**
     * Workaround to invoke the private flatten method without reflection inside test. Mirrors the signature of the private method.
     */
    private static class ConfigurationFactoryTestHelper {

        // Delegate to the same logic in ConfigurationFactory.flatten
        private static void flatten(
              Map<String, Object> nested,
              Map<String, String> flat
        ) {
            // call the private method via reflection
            try {
                var m = ConfigurationFactory.class
                      .getDeclaredMethod("flatten", String.class, Map.class, Map.class);
                m.setAccessible(true);
                m.invoke(null, "", nested, flat);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}