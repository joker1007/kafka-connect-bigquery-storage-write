package com.reproio.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

class RecordConverterTest {

  @Test
  void testExtractJsonObject() {
    var innerSchema =
        SchemaBuilder.struct()
            .field("inner1", Schema.STRING_SCHEMA)
            .field("inner2", Schema.BOOLEAN_SCHEMA)
            .build();
    var schema =
        SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("int_value", Schema.INT64_SCHEMA)
            .field("double_value", Schema.FLOAT64_SCHEMA)
            .field("boolean_value", Schema.BOOLEAN_SCHEMA)
            .field("array_value", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("map_value", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
            .field("struct_value", innerSchema)
            .field("optional_array_value", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())
            .build();

    var innerStruct = new Struct(innerSchema);
    innerStruct.put("inner1", "inner_value").put("inner2", false);
    var struct = new Struct(schema);
    struct
        .put("id", "id")
        .put("int_value", 12345L)
        .put("double_value", 0.123)
        .put("boolean_value", true)
        .put("array_value", List.of("a", "b", "c"))
        .put("map_value", Map.of("k1", 1, "k2", 2, "k3", 3))
        .put("struct_value", innerStruct)
        .put("optional_array_value", null);

    var result = RecordConverter.extractJsonObject(struct, schema);
    assertInstanceOf(JSONObject.class, result);
    assertNotNull(result);
    var jsonObject = (JSONObject) result;
    var expectedJSON =
        "{\n"
            + "  \"boolean_value\": true,\n"
            + "  \"map_value\": {\n"
            + "    \"k1\": 1,\n"
            + "    \"k2\": 2,\n"
            + "    \"k3\": 3\n"
            + "  },\n"
            + "  \"array_value\": [\n"
            + "    \"a\",\n"
            + "    \"b\",\n"
            + "    \"c\"\n"
            + "  ],\n"
            + "  \"id\": \"id\",\n"
            + "  \"double_value\": 0.123,\n"
            + "  \"struct_value\": {\n"
            + "    \"inner2\": false,\n"
            + "    \"inner1\": \"inner_value\"\n"
            + "  },\n"
            + "  \"int_value\": 12345\n"
            + "}";

    JSONAssert.assertEquals(expectedJSON, jsonObject.toString(), true);
  }
}
