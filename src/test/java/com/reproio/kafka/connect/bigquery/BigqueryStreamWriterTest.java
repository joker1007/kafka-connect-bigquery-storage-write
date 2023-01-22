package com.reproio.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializtionError;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.reproio.kafka.connect.bigquery.BigqueryStreamWriter.AppendContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

class BigqueryStreamWriterTest {

  private final Schema valueSchema =
      SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("int_value", Schema.INT64_SCHEMA)
          .build();

  private long sinkRecordOffset = 0;

  private BigQueryWriteClient mockedClient;
  private JsonStreamWriter mockedStreamWriter;
  private AppendRowsResponse mockedApiResponse;

  private SinkRecord buildSinkRecord() {
    var struct = new Struct(valueSchema);
    struct.put("id", "id-" + sinkRecordOffset);
    struct.put("int_value", 123L);

    var sinkRecord =
        new SinkRecord(
            "topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, struct, sinkRecordOffset);
    sinkRecordOffset++;
    return sinkRecord;
  }

  @Test
  void testAppendRecord() {
    this.mockedClient = mock(BigQueryWriteClient.class);
    var writer = new BigqueryStreamWriter("bq_project", "bq_dataset", "bq_table", mockedClient);

    SinkRecord sinkRecord = buildSinkRecord();
    writer.appendRecord(sinkRecord);

    assertEquals(sinkRecord, writer.currentBufferChunk.get(0));
  }

  @Test
  void testAppendRecordWith1001Record() {
    this.mockedClient = mock(BigQueryWriteClient.class);
    var writer =
        spy(new BigqueryStreamWriter("bq_project", "bq_dataset", "bq_table", mockedClient));

    when(writer.write())
        .then(
            (invocation) -> {
              writer.currentBufferChunk.clear();
              return Optional.empty();
            })
        .thenReturn(Optional.empty());

    assertFalse(writer.isExceedRecordLimit());
    IntStream.rangeClosed(0, 1000)
        .forEach(
            i -> {
              var struct = new Struct(valueSchema);
              struct.put("id", "id-" + i);
              struct.put("int_value", (long) i * 100);

              var sinkRecord =
                  new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, struct, i);
              writer.appendRecord(sinkRecord);
            });
    assertTrue(writer.isExceedRecordLimit());
  }

  @SneakyThrows
  private BigqueryStreamWriter getMockedWriter() {
    this.mockedClient = mock(BigQueryWriteClient.class);
    var writer =
        spy(new BigqueryStreamWriter("bq_project", "bq_dataset", "bq_table", mockedClient));
    this.mockedStreamWriter = mock(JsonStreamWriter.class);
    when(mockedStreamWriter.getStreamName()).thenReturn("stream-name");
    this.mockedApiResponse = mock(AppendRowsResponse.class);
    SettableApiFuture<AppendRowsResponse> apiFuture = SettableApiFuture.create();
    apiFuture.set(mockedApiResponse);
    when(mockedStreamWriter.append(any(), anyLong())).thenReturn(apiFuture);
    writer.setStreamWriter(mockedStreamWriter);

    return writer;
  }

  private void assertAppendContext(
      AppendContext appendContext,
      int expectedStreamOffset,
      List<Long> expectedWrittenRecordKafkaOffsets,
      List<Long> expectedErrorRecordKafkaOffsets) {

    assertNotNull(appendContext.getAppendRowsResponse());
    assertEquals(expectedStreamOffset, appendContext.getWrittenWriterStreamOffset());
    assertEquals(expectedWrittenRecordKafkaOffsets, appendContext.getWrittenRecordKafkaOffsets());
    assertEquals(expectedErrorRecordKafkaOffsets, appendContext.getErrorRecordKafkaOffsets());
  }

  @Test
  void testWrite() {
    var writer = getMockedWriter();

    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());

    assertEquals(2, writer.currentBufferChunk.size());

    Optional<AppendContext> appendContext = writer.write();
    assertTrue(appendContext.isPresent());
    assertAppendContext(appendContext.get(), 0, List.of(0L, 1L), List.of());
    assertEquals(0, writer.currentBufferChunk.size());

    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());

    assertEquals(2, writer.currentBufferChunk.size());

    appendContext = writer.write();
    assertTrue(appendContext.isPresent());
    assertAppendContext(appendContext.get(), 2, List.of(2L, 3L), List.of());
    assertEquals(0, writer.currentBufferChunk.size());
  }

  @SneakyThrows
  @Test
  void testWriteWithAppendSerializationError() {
    var writer = getMockedWriter();

    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());

    assertEquals(3, writer.currentBufferChunk.size());

    var streamName = mockedStreamWriter.getStreamName();
    SettableApiFuture<AppendRowsResponse> apiFuture = SettableApiFuture.create();
    apiFuture.set(mockedApiResponse);

    when(mockedStreamWriter.append(any(), anyLong())).thenReturn(apiFuture);
    when(mockedStreamWriter.append(any(), anyLong()))
        .then(
            invocation -> {
              var array = (JSONArray) invocation.getArgument(0);
              if (array.length() == 3) {
                // codeValue = 3 is INVALID_ARGUMENT. see: io.grpc.Status.Code
                throw new AppendSerializtionError(
                    3, "serialization error", streamName, Map.of(1, "serialization error"));
              } else {
                return apiFuture;
              }
            });

    Optional<AppendContext> appendContext = writer.write();
    assertTrue(appendContext.isPresent());
    assertAppendContext(appendContext.get(), 0, List.of(0L, 2L), List.of(1L));
    assertEquals(0, writer.currentBufferChunk.size());
  }

  @Test
  void testReset() {
    var writer = getMockedWriter();
    writer.appendRecord(buildSinkRecord());

    var response = mock(FinalizeWriteStreamResponse.class);
    when(mockedClient.finalizeWriteStream(mockedStreamWriter.getStreamName())).thenReturn(response);

    assertFalse(writer.currentBufferChunk.isEmpty());

    writer.reset();

    verify(mockedClient).finalizeWriteStream(mockedStreamWriter.getStreamName());
    verify(mockedStreamWriter).close();
    assertTrue(writer.currentBufferChunk.isEmpty());
  }

  @Test
  void testCommit() {
    var writer = getMockedWriter();
    writer.appendRecord(buildSinkRecord());

    var finalizeResponse = mock(FinalizeWriteStreamResponse.class);
    when(mockedClient.finalizeWriteStream(mockedStreamWriter.getStreamName()))
        .thenReturn(finalizeResponse);

    var commitResponse = mock(BatchCommitWriteStreamsResponse.class);
    when(commitResponse.hasCommitTime()).thenReturn(true);
    when(mockedClient.batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class)))
        .thenReturn(commitResponse);

    writer.commit();

    verify(mockedClient).finalizeWriteStream(mockedStreamWriter.getStreamName());
    verify(mockedClient).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
    verify(mockedStreamWriter).close();
  }
}
