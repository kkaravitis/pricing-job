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
package com.wordpress.kkaravitis.pricing.infrastructure.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ParameterTool;

/**
 * @author Konstantinos Karavitis
 */
public class ConfigurationFactory {

    public Configuration build(ParameterTool params) throws IOException {
        String configDir = params.get("configLocation", null);

        InputStream in;
        if (configDir != null) {
            in = Files.newInputStream(Paths.get(configDir, "config.yaml"));
        } else {
            in = ConfigurationFactory.class
                  .getClassLoader()
                  .getResourceAsStream("config.yaml");
        }

        // 3) Parse YAML into a Map<String,Object>
        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        Map<String, Object> yaml = yamlReader.readValue(in, new TypeReference<Map<String, Object>>() {});

        // 4) Flatten nested maps into dotted keys
        Map<String, String> flat = new HashMap<>();
        flatten("", yaml, flat);

        // 5) Turn into a Flink Configuration
        Configuration flinkConfig = new Configuration();
        flat.forEach(flinkConfig::setString);

        return flinkConfig;

    }

    // recursive helper
    private static void flatten(String prefix,  Map<?,?> map, Map<String, String> out) {
        map.forEach((yamlKey, value) -> {
            String key = prefix.isEmpty() ? (String)yamlKey : prefix + "." + yamlKey;
            if (value instanceof Map) {
                Map<?,?> child = (Map<?,?>) value;
                flatten(key, child, out);
            } else {
                out.put(key, value.toString());
            }
        });
    }
}
