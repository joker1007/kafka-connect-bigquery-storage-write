package com.reproio.kafka.connect.bigquery;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONArray;
import org.json.JSONObject;

public class RecordConverter {
  private RecordConverter() {}

  public static Object extractJsonObject(Object value, Schema schema) {
    Schema.Type schemaType;
    boolean isOptional;
    if (schema == null) {
      schemaType = ConnectSchema.schemaType(value.getClass());
      isOptional = true;
    } else {
      schemaType = schema.type();
      isOptional = schema.isOptional();
    }

    if (value == null) {
      if (isOptional) {
        return null;
      } else {
        throw new UnsupportedDataTypeException("Not optional field has null value");
      }
    }

    switch (schemaType) {
      case STRUCT:
        if (schema == null) {
          throw new UnsupportedDataTypeException("Cannot find Struct Schema");
        }
        var struct = (Struct) value;
        var jsonObject1 = new JSONObject();
        schema
            .fields()
            .forEach(
                field -> {
                  var fieldValue = extractJsonObject(struct.get(field), field.schema());
                  jsonObject1.put(field.name(), fieldValue);
                });
        return jsonObject1;
      case MAP:
        var map = (Map<?, ?>) value;
        var jsonObject2 = new JSONObject();
        map.forEach(
            (key, mapValue) -> {
              if (key instanceof String) {
                jsonObject2.put(
                    (String) key,
                    extractJsonObject(mapValue, schema == null ? null : schema.valueSchema()));
              } else {
                throw new UnsupportedDataTypeException("Map field has non-string key");
              }
            });
        return jsonObject2;
      case ARRAY:
        var collection = (Collection<?>) value;
        var jsonArray = new JSONArray();
        for (var item : collection) {
          jsonArray.put(extractJsonObject(item, schema == null ? null : schema.valueSchema()));
        }
        return jsonArray;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
        return value;
      case STRING:
        return ((CharSequence) value).toString();
      case BYTES:
        throw new UnsupportedDataTypeException("Bytes field is unsupported");
      default:
        throw new UnsupportedDataTypeException(String.format("Unknown data type: %s", schemaType));
    }
  }
}
