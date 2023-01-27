package com.reproio.kafka.connect.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializtionError;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status.Code;
import java.io.Closeable;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;

@Slf4j
public class BigqueryStreamWriter implements Closeable {
  private final BigQueryWriteClient client;
  private final TableName tableName;
  private final WriteMode writeMode;

  private JsonStreamWriter streamWriter;

  private int currentOffset;

  private static final int BUFFER_CHUNK_RECORD_LIMIT = 1000;
  @VisibleForTesting final List<SinkRecord> currentBufferChunk = new ArrayList<>();

  private final Phaser inflightRequests = new Phaser(1);

  public enum WriteMode {
    PENDING,
    COMMITTED;
  }

  @SneakyThrows
  public static BigqueryStreamWriter create(
      String project, String dataset, String table, WriteMode writeMode, String keyfile) {
    var keyFileStream = new FileInputStream(keyfile);
    var writeSettings =
        BigQueryWriteSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(GoogleCredentials.fromStream(keyFileStream)))
            .build();
    var client = BigQueryWriteClient.create(writeSettings);
    return new BigqueryStreamWriter(project, dataset, table, writeMode, client);
  }

  @VisibleForTesting
  public BigqueryStreamWriter(
      String project,
      String dataset,
      String table,
      WriteMode writeMode,
      BigQueryWriteClient client) {
    this.tableName = TableName.of(project, dataset, table);
    this.writeMode = writeMode;
    this.client = client;
  }

  public void waitAllInflightRequests() {
    log.info("Wait all inflight requets");
    inflightRequests.arriveAndAwaitAdvance();
  }

  public void close() {
    streamWriter.close();
    client.close();
  }

  @Data
  @RequiredArgsConstructor
  static class AppendContext {
    @NonNull private final List<Long> writtenRecordKafkaOffsets;
    @NonNull private final List<Long> errorRecordKafkaOffsets;
    @NonNull private final String streamName;
    private final int writtenWriterStreamOffset;
    @Nullable private AppendRowsResponse appendRowsResponse;
    @Nullable private Throwable error;
    @Nullable private StorageException storageException;
    private int retryCount;

    private static final List<Code> RETRIABLE_ERROR_CODES =
        List.of(
            Code.INTERNAL,
            Code.ABORTED,
            Code.CANCELLED,
            Code.FAILED_PRECONDITION,
            Code.DEADLINE_EXCEEDED,
            Code.UNAVAILABLE);

    public void setError(Throwable error) {
      this.error = error;
      this.storageException = Exceptions.toStorageException(error);
    }

    private Code storageErrorCode() {
      return storageException == null ? Code.UNKNOWN : storageException.getStatus().getCode();
    }

    public long getFirstKafkaOffset() {
      return writtenRecordKafkaOffsets.get(0);
    }

    public long getLastKafkaOffset() {
      return writtenRecordKafkaOffsets.get(writtenRecordKafkaOffsets.size() - 1);
    }

    public boolean hasError() {
      return error != null
          || (appendRowsResponse != null && appendRowsResponse.getRowErrorsCount() > 0);
    }

    public boolean hasUnretryableError() {
      return error != null && !RETRIABLE_ERROR_CODES.contains(storageErrorCode());
    }

    public boolean isAlreadyExists() {
      return storageErrorCode() == Code.ALREADY_EXISTS;
    }

    public boolean isOutOfRange() {
      return storageErrorCode() == Code.OUT_OF_RANGE;
    }

    public List<Long> corruptedRowKafkaOffsets() {
      log.debug("Get corruptedRowKafkaOffsets");
      if (hasUnretryableError()) {
        log.debug("Got corruptedRowKafkaOffsets: {}", writtenRecordKafkaOffsets);
        return writtenRecordKafkaOffsets;
      } else if (appendRowsResponse != null && appendRowsResponse.getRowErrorsCount() > 0) {
        log.debug("Row Errors: {}", appendRowsResponse.getRowErrorsList());
        var offsets =
            appendRowsResponse.getRowErrorsList().stream()
                .map(rowError -> writtenRecordKafkaOffsets.get((int) rowError.getIndex()))
                .collect(Collectors.toList());
        log.debug("Got corruptedRowKafkaOffsets: {}", offsets);
        return offsets;
      } else {
        return List.of();
      }
    }
  }

  @VisibleForTesting
  void setStreamWriter(JsonStreamWriter streamWriter) {
    this.streamWriter = streamWriter;
  }

  @SneakyThrows
  private void createWriteStream() {
    log.debug("Create WriteStream");
    var stream =
        WriteStream.newBuilder()
            .setType(writeMode == WriteMode.PENDING ? Type.PENDING : Type.COMMITTED)
            .build();

    var createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(tableName.toString())
            .setWriteStream(stream)
            .build();
    WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

    this.streamWriter =
        JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client)
            .setIgnoreUnknownFields(true)
            .build();
    this.currentOffset = 0;
  }

  public void appendRecord(SinkRecord record) {
    if (record.value() instanceof Struct) {
      currentBufferChunk.add(record);
    } else {
      log.warn("record is ignored because it is not struct record");
    }
  }

  public boolean isExceedRecordLimit() {
    return currentBufferChunk.size() >= BUFFER_CHUNK_RECORD_LIMIT;
  }

  @SneakyThrows
  public Optional<AppendContext> write() {
    if (currentBufferChunk.isEmpty()) {
      return Optional.empty();
    }

    if (streamWriter == null) {
      createWriteStream();
    }

    var jsonArray = new JSONArray();
    List<Long> writtenRecordOffsets = new ArrayList<>();
    try {
      for (SinkRecord sinkRecord : currentBufferChunk) {
        jsonArray.put(convertToJson(sinkRecord));
        writtenRecordOffsets.add(sinkRecord.kafkaOffset());
      }

      return sendPayload(jsonArray, writtenRecordOffsets, List.of());
    } catch (AppendSerializtionError e) {
      log.error("Failed to serialize:", e);
      Map<Integer, String> rowIndexToErrorMessage = e.getRowIndexToErrorMessage();
      var newArray = new JSONArray();
      List<Long> newWrittenRecordOffsets = new ArrayList<>();
      List<Long> errorRecordOffsets = new ArrayList<>();
      for (int i = 0; i < currentBufferChunk.size(); i++) {
        if (!rowIndexToErrorMessage.containsKey(i)) {
          newArray.put(jsonArray.get(i));
          newWrittenRecordOffsets.add(writtenRecordOffsets.get(i));
        } else {
          log.error(
              "Row Error: {}, record={}", rowIndexToErrorMessage.get(i), currentBufferChunk.get(i));
          errorRecordOffsets.add(writtenRecordOffsets.get(i));
        }
      }

      if (newWrittenRecordOffsets.isEmpty()) {
        var appendContext =
            new AppendContext(
                writtenRecordOffsets, List.of(), streamWriter.getStreamName(), currentOffset);
        appendContext.setError(e);
        return Optional.of(appendContext);
      }
      return sendPayload(newArray, newWrittenRecordOffsets, errorRecordOffsets);
    }
  }

  @SneakyThrows
  private Optional<AppendContext> sendPayload(
      JSONArray jsonArray, List<Long> writtenRecordOffsets, List<Long> errorRecordOffsets) {

    var appendContext =
        new AppendContext(
            writtenRecordOffsets, errorRecordOffsets, streamWriter.getStreamName(), currentOffset);

    log.debug("Send Payload: {}", appendContext);
    ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArray, currentOffset);
    ApiFutures.addCallback(
        future, new AppendCompleteCallback(appendContext), MoreExecutors.directExecutor());
    inflightRequests.register();
    this.currentOffset += writtenRecordOffsets.size();
    log.debug("Update current offset: {}", currentOffset);
    currentBufferChunk.clear();
    return Optional.of(appendContext);
  }

  private JSONObject convertToJson(SinkRecord record) {
    log.trace("Attempt to convert record: {}", record);
    JSONObject jsonObject =
        (JSONObject) RecordConverter.extractJsonObject(record.value(), record.valueSchema());
    log.trace("Converted record: {}", jsonObject);
    return jsonObject;
  }

  private void finalizeStream() {
    if (streamWriter != null) {
      log.info("Attempt to Finalize Stream {}", streamWriter.getStreamName());
      try {
        FinalizeWriteStreamResponse response =
            client.finalizeWriteStream(streamWriter.getStreamName());
        log.info(
            "Finalize Stream: written: {}, {} records",
            streamWriter.getStreamName(),
            response.getRowCount());
      } catch (Exception e) {
        log.error("Failed to finalize stream {}", streamWriter.getStreamName(), e);
      }
    }
  }

  private void commitStream() {
    log.info("Attempt to commit Stream {}", streamWriter.getStreamName());
    var commitRequest =
        BatchCommitWriteStreamsRequest.newBuilder()
            .setParent(tableName.toString())
            .addWriteStreams(streamWriter.getStreamName())
            .build();
    BatchCommitWriteStreamsResponse response = client.batchCommitWriteStreams(commitRequest);
    if (response.hasCommitTime()) {
      log.info(
          "Commit Stream {}: commit_time={}",
          streamWriter.getStreamName(),
          response.getCommitTime());
    } else {
      log.error("Commit failed");
      throw new RuntimeException(response.getStreamErrors(0).getErrorMessage());
    }
  }

  public void reset() {
    finalizeStream();
    log.debug("Clear streamWriter");
    streamWriter.close();
    this.streamWriter = null;
    this.currentBufferChunk.clear();
  }

  public void commit() {
    if (writeMode == WriteMode.PENDING) {
      finalizeStream();
      commitStream();
      log.debug("Clear streamWriter");
      streamWriter.close();
      this.streamWriter = null;
    }
  }

  class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
    private final AppendContext context;

    public AppendCompleteCallback(AppendContext context) {
      this.context = context;
    }

    @Override
    public void onFailure(Throwable t) {
      log.error("Failed to write rows", t);
      context.setError(t);
      done();
    }

    @Override
    public void onSuccess(AppendRowsResponse result) {
      if (log.isTraceEnabled()) {
        log.trace(
            "Appended Rows to WriterStream: {size={}, offset={}}",
            result.getAppendResult().getSerializedSize(),
            result.getAppendResult().getOffset());
      }
      context.setAppendRowsResponse(result);
      done();
    }

    private void done() {
      inflightRequests.arriveAndDeregister();
    }
  }
}
