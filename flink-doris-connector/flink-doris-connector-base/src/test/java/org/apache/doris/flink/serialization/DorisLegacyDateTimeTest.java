// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.models.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DorisLegacyDateTimeTest {
    @Test
    void decodesRecordedDoris215Samples() throws Exception {
        JsonNode samples =
                new ObjectMapper()
                        .readTree(
                                getClass().getResourceAsStream("/datetime/doris-2.1.5-wire.json"));
        assertThat(samples.size()).isEqualTo(80);
        for (JsonNode sample : samples) {
            String name = sample.get("field").asText();
            boolean string = sample.get("declaredUnit").isNull();
            ArrowType type =
                    string
                            ? ArrowType.Utf8.INSTANCE
                            : new ArrowType.Timestamp(
                                    TimeUnit.valueOf(sample.get("declaredUnit").asText()), null);
            Field field = new Field(name, FieldType.nullable(type), null);
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    FieldVector vector = field.createVector(allocator)) {
                vector.allocateNew();
                if (!sample.get("raw").isNull()) {
                    if (string) {
                        ((VarCharVector) vector)
                                .setSafe(
                                        0,
                                        sample.get("raw")
                                                .asText()
                                                .getBytes(StandardCharsets.UTF_8));
                    } else {
                        ((TimeStampVector) vector)
                                .setSafe(0, Long.parseLong(sample.get("raw").asText()));
                    }
                }
                vector.setValueCount(1);
                try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                    root.setRowCount(1);
                    ArrowReader reader = mock(ArrowReader.class);
                    when(reader.getVectorSchemaRoot()).thenReturn(root);
                    Schema schema = new Schema();
                    schema.put(name, sample.get("dorisType").asText(), "", 0, 0, "");
                    String actualUnit = string ? "SECOND" : sample.get("actualUnit").asText();
                    DorisReadOptions options =
                            DorisReadOptions.builder()
                                    .setLegacyDateTimeZone("Asia/Shanghai")
                                    .setLegacyDateTimeUnits(
                                            Collections.singletonMap(name, actualUnit))
                                    .build();
                    RowBatch batch = new RowBatch(reader, schema, false, options);
                    try {
                        Object expected =
                                sample.get("expected").isNull()
                                        ? null
                                        : LocalDateTime.parse(
                                                sample.get("expected").asText().replace(' ', 'T'));
                        assertThat(batch.readFlightArrow().next().get(0))
                                .as(
                                        sample.get("protocol").asText()
                                                + ":"
                                                + name
                                                + ":"
                                                + sample.get("rowId"))
                                .isEqualTo(expected);
                    } finally {
                        batch.close();
                    }
                }
            }
        }
    }

    @Test
    void preservesNativeAndTimestampTzBehavior() throws Exception {
        long raw = 1767294245123456L;
        for (String timezone : new String[] {null, "UTC"}) {
            Field field =
                    new Field(
                            "ts",
                            FieldType.nullable(
                                    new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone)),
                            null);
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    FieldVector vector = field.createVector(allocator);
                    VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                vector.allocateNew();
                ((TimeStampVector) vector).setSafe(0, raw);
                root.setRowCount(1);
                ArrowReader reader = mock(ArrowReader.class);
                when(reader.getVectorSchemaRoot()).thenReturn(root);
                Schema schema = new Schema();
                schema.put("ts", "TIMESTAMPTZ", "", 0, 0, "");
                DorisReadOptions legacy =
                        DorisReadOptions.builder()
                                .setLegacyDateTimeZone("Asia/Shanghai")
                                .setLegacyDateTimeUnits(Collections.singletonMap("ts", "SECOND"))
                                .build();
                RowBatch batch = new RowBatch(reader, schema, false, legacy);
                try {
                    Object nativeExpected = batch.getDateTime(0, vector);
                    assertThat(batch.readFlightArrow().next().get(0)).isEqualTo(nativeExpected);
                } finally {
                    batch.close();
                }
                schema = new Schema();
                schema.put("ts", "DATETIMEV2", "", 0, 0, "");
                batch = new RowBatch(reader, schema, false, DorisReadOptions.defaults());
                try {
                    assertThat(batch.readFlightArrow().next().get(0))
                            .isEqualTo(batch.getDateTime(0, vector));
                } finally {
                    batch.close();
                }
            }
        }
    }

    @Test
    void rejectsMissingUnitAndConflictingTimezoneBeforeReadingValues() throws Exception {
        for (String timezone : new String[] {null, "UTC"}) {
            Field field =
                    new Field(
                            "ts",
                            FieldType.nullable(
                                    new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone)),
                            null);
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    FieldVector vector = field.createVector(allocator);
                    VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                vector.allocateNew();
                root.setRowCount(1);
                ArrowReader reader = mock(ArrowReader.class);
                when(reader.getVectorSchemaRoot()).thenReturn(root);
                Schema schema = new Schema();
                schema.put("ts", "DATETIMEV2", "", 0, 0, "");
                DorisReadOptions legacy =
                        DorisReadOptions.builder().setLegacyDateTimeZone("Asia/Shanghai").build();
                RowBatch batch = new RowBatch(reader, schema, false, legacy);
                try {
                    assertThatThrownBy(batch::readFlightArrow)
                            .hasMessageContaining(
                                    timezone == null
                                            ? "Missing source.datetime.legacy-units"
                                            : "timezone-free");
                } finally {
                    batch.close();
                }
            }
        }
    }
}
