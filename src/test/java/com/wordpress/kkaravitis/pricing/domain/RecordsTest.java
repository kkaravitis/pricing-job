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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.Test;


class RecordsTest {

    @Test
    public void test() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = """ 
              {
                "orderId" : "12331123",
                "productId" : "213",
                "quantity" : 1,
                "timestamp" : 123123 
              }
              """;
        OrderEvent event = objectMapper.readValue(json, OrderEvent.class);
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    static class TestOrder {
        private String orderId;
        private String productId;
        private Integer quantity;
        private Long timestamp;
    }



}
