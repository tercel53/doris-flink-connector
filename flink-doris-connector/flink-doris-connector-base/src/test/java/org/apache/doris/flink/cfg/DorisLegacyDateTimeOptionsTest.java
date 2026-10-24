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

package org.apache.doris.flink.cfg;

import org.apache.arrow.vector.types.TimeUnit;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisLegacyDateTimeOptionsTest {
    @Test
    void validatesContractOnceAtBuild() {
        assertThat(DorisReadOptions.defaults().getLegacyDateTimeZone()).isNull();
        assertThatThrownBy(
                        () ->
                                DorisReadOptions.builder()
                                        .setLegacyDateTimeUnits(
                                                Collections.singletonMap("ts", "SECOND"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(
                        () ->
                                DorisReadOptions.builder()
                                        .setLegacyDateTimeZone("unknown-zone")
                                        .build())
                .isInstanceOf(java.time.DateTimeException.class);
        for (String unit : new String[] {"NANOSECOND", "guessed"}) {
            assertThatThrownBy(
                            () ->
                                    DorisReadOptions.builder()
                                            .setLegacyDateTimeZone("Asia/Shanghai")
                                            .setLegacyDateTimeUnits(
                                                    Collections.singletonMap("ts", unit))
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void freezesAndPreservesContractAcrossCopyAndJavaSerialization() throws Exception {
        Map<String, String> units = new HashMap<>();
        units.put("ts", "SECOND");
        DorisReadOptions options =
                DorisReadOptions.builder()
                        .setLegacyDateTimeZone("Asia/Shanghai")
                        .setLegacyDateTimeUnits(units)
                        .build();
        units.put("ts", "MILLISECOND");
        assertThat(options.getLegacyDateTimeUnits()).containsEntry("ts", TimeUnit.SECOND);
        assertThat(options.copy()).isEqualTo(options).hasSameHashCodeAs(options);
        assertThat(options).isNotEqualTo(DorisReadOptions.defaults());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(options);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(options);
        }
        assertThatThrownBy(() -> options.getLegacyDateTimeUnits().put("other", TimeUnit.SECOND))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void propagatesPropertiesMapSyntax() {
        Properties properties = new Properties();
        properties.setProperty("fenodes", "127.0.0.1:8030");
        properties.setProperty("table.identifier", "db.tbl");
        properties.setProperty("source.datetime.legacy-time-zone", "Asia/Shanghai");
        properties.setProperty(
                "source.datetime.legacy-units", "created_at:SECOND,updated_at:MICROSECOND");
        DorisReadOptions options = new DorisStreamOptions(properties).getReadOptions();
        assertThat(options.getLegacyDateTimeZone()).isEqualTo(ZoneId.of("Asia/Shanghai"));
        assertThat(options.getLegacyDateTimeUnits())
                .containsEntry("created_at", TimeUnit.SECOND)
                .containsEntry("updated_at", TimeUnit.MICROSECOND);
    }
}
