package com.reproio.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.reproio.kafka.connect.bigquery.BigqueryStreamWriter.AppendContext;
import com.reproio.kafka.connect.bigquery.BigqueryStreamWriter.WriteMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

class BigqueryStorageWriteSinkTaskTest {

  private static final Map<String, String> VALID_CONFIG =
      Map.of(
          "project",
          "bq_project",
          "dataset",
          "bq_dataset",
          "table",
          "bq_table",
          "keyfile",
          "/tmp/dummy_key",
          "write_mode",
          "pending");

  private final Schema valueSchema =
      SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("int_value", Schema.INT64_SCHEMA)
          .build();
  private final String topicName = "test-topic";

  private long sinkRecordOffset = 0;

  private BigqueryStorageWriteSinkTask task;
  private BigqueryStreamWriter mockedWriter;
  private SinkTaskContext mockedContext;

  private void setUpTask() {
    this.task = new BigqueryStorageWriteSinkTask();
    this.mockedContext = mock(SinkTaskContext.class);
    task.initialize(mockedContext);
    task.start(VALID_CONFIG);
    var topicPartitions = List.of(new TopicPartition(topicName, 0));
    try (var staticMock = mockStatic(BigqueryStreamWriter.class)) {
      this.mockedWriter = mock(BigqueryStreamWriter.class);
      staticMock
          .when(
              () ->
                  BigqueryStreamWriter.create(
                      "bq_project", "bq_dataset", "bq_table", WriteMode.PENDING, "/tmp/dummy_key"))
          .thenReturn(mockedWriter);
      task.open(topicPartitions);
    }
  }

  @Test
  void testStart() {
    var task = new BigqueryStorageWriteSinkTask();
    assertDoesNotThrow(() -> task.start(VALID_CONFIG));
  }

  @Test
  void testWithInvalidConfig() {
    var task = new BigqueryStorageWriteSinkTask();
    var config = Map.of("dataset", "bq_dataset", "table", "bq_table", "keyfile", "/tmp/dummy_key");
    assertThrows(ConfigException.class, () -> task.start(config));
  }

  private SinkRecord buildSinkRecord() {
    var struct = new Struct(valueSchema);
    struct.put("id", "id-" + sinkRecordOffset);
    struct.put("int_value", 123L);

    var sinkRecord =
        new SinkRecord(
            topicName,
            0,
            Schema.STRING_SCHEMA,
            "key-" + sinkRecordOffset,
            valueSchema,
            struct,
            sinkRecordOffset);
    sinkRecordOffset++;
    return sinkRecord;
  }

  @Test
  void testPut() {
    setUpTask();

    var records = List.of(buildSinkRecord(), buildSinkRecord());
    task.put(records);
    verify(mockedWriter).appendRecord(records.get(0));
    verify(mockedWriter).appendRecord(records.get(1));
  }

  @Test
  void testPutWithCorruptedRowOffsets() {
    setUpTask();

    task.getCorruptedRowOffsets(new TopicPartition(topicName, 0)).add(0L);
    var records = List.of(buildSinkRecord(), buildSinkRecord());
    task.put(records);
    verify(mockedWriter, never()).appendRecord(records.get(0));
    verify(mockedWriter).appendRecord(records.get(1));
  }

  @Test
  void testFlush() {
    setUpTask();

    var mockedAppendContext = mock(AppendContext.class);
    when(mockedWriter.write()).thenReturn(Optional.of(mockedAppendContext));
    task.flush(Map.of(new TopicPartition(topicName, 0), new OffsetAndMetadata(0)));

    verify(mockedWriter).write();
  }

  @Test
  void testPreCommit() {
    setUpTask();

    var records = List.of(buildSinkRecord(), buildSinkRecord());
    task.put(records);

    var topicPartition = new TopicPartition(topicName, 0);

    var mockedAppendContext = mock(AppendContext.class);
    when(mockedWriter.write()).thenReturn(Optional.of(mockedAppendContext));
    var offsets = task.preCommit(Map.of(topicPartition, new OffsetAndMetadata(2)));

    verify(mockedWriter).write();
    verify(mockedWriter).commit();
    verify(mockedWriter, never()).reset();
    assertTrue(task.getInflightContexts(topicPartition).isEmpty());
    assertEquals(2, offsets.get(topicPartition).offset());
  }

  @Test
  void testPreCommitWithCorruptedRows() {
    setUpTask();

    var records = List.of(buildSinkRecord(), buildSinkRecord());
    task.put(records);

    var topicPartition = new TopicPartition(topicName, 0);

    var mockedAppendContext = mock(AppendContext.class);
    when(mockedAppendContext.corruptedRowKafkaOffsets()).thenReturn(List.of(0L, 1L));
    when(mockedAppendContext.hasError()).thenReturn(true);
    when(mockedWriter.write()).thenReturn(Optional.of(mockedAppendContext));
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    currentOffsets.put(topicPartition, new OffsetAndMetadata(2));
    var offsets = task.preCommit(currentOffsets);

    verify(mockedWriter).write();
    verify(mockedWriter, never()).commit();
    verify(mockedWriter).reset();
    assertTrue(task.getInflightContexts(topicPartition).isEmpty());
    assertEquals(Set.of(0L, 1L), task.getCorruptedRowOffsets(topicPartition));
    assertEquals(0, offsets.get(topicPartition).offset());
  }
}
