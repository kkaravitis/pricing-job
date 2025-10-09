/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
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
