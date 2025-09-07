package com.reproio.kafka.connect.bigquery;

import com.google.common.annotations.VisibleForTesting;
import com.reproio.kafka.connect.bigquery.BigqueryStreamWriter.AppendContext;
import com.reproio.kafka.connect.bigquery.BigqueryStreamWriter.WriteMode;
import com.reproio.kafka.connect.bigquery.utils.Version;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

@Slf4j
public class BigqueryStorageWriteSinkTask extends SinkTask {
  private Map<TopicPartition, BigqueryStreamWriter> topicPartitionWriters;
  private Map<TopicPartition, List<AppendContext>> inflightContexts;
  private Map<TopicPartition, Set<Long>> corruptedRowOffsetsTable;

  private BigqueryStreamWriteSinkConfig config;

  private final Map<TopicPartition, NavigableSet<Long>> retryBoundaries = new HashMap<>();

  private volatile boolean stopped;
  private volatile boolean closed;

  @Override
  public String version() {
    return Version.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.topicPartitionWriters = new HashMap<>();
    this.inflightContexts = new HashMap<>();
    this.corruptedRowOffsetsTable = new HashMap<>();
    this.config = new BigqueryStreamWriteSinkConfig(props);
    log.trace("task.start: {}", props);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    super.open(partitions);
    var project = config.getString(BigqueryStreamWriteSinkConfig.PROJECT_CONFIG);
    var dataset = config.getString(BigqueryStreamWriteSinkConfig.DATASET_CONFIG);
    var table = config.getString(BigqueryStreamWriteSinkConfig.TABLE_CONFIG);
    var keyfile = config.getString(BigqueryStreamWriteSinkConfig.KEYFILE_CONFIG);
    var writeMode =
        WriteMode.valueOf(
            config.getString(BigqueryStreamWriteSinkConfig.WRITE_MODE_CONFIG).toUpperCase());
    var bufferSize = config.getInt(BigqueryStreamWriteSinkConfig.BUFFER_SIZE_CONFIG);
    partitions.forEach(
        topicPartition -> {
          var writer =
              BigqueryStreamWriter.create(project, dataset, table, writeMode, keyfile, bufferSize);
          topicPartitionWriters.put(topicPartition, writer);
        });
    closed = false;
    log.trace("task.open: {}", topicPartitionWriters);
  }

  @VisibleForTesting
  Set<Long> getCorruptedRowOffsets(TopicPartition topicPartition) {
    return corruptedRowOffsetsTable.computeIfAbsent(topicPartition, key -> new HashSet<>());
  }

  @VisibleForTesting
  List<AppendContext> getInflightContexts(TopicPartition topicPartition) {
    return inflightContexts.computeIfAbsent(topicPartition, key -> new ArrayList<>());
  }

  NavigableSet<Long> getRetryBoundaries(TopicPartition topicPartition) {
    return retryBoundaries.computeIfAbsent(topicPartition, key -> new TreeSet<>());
  }

  private void errorReport(SinkRecord record) {
    if (context.errantRecordReporter() != null) {
      context.errantRecordReporter().report(record, new CorruptedRowException());
    } else {
      log.trace("Detect Corrupted Row: {}", record);
    }
  }

  private boolean matchRetryBoundary(TopicPartition topicPartition, SinkRecord record) {
    return getRetryBoundaries(topicPartition).contains(record.kafkaOffset());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      log.trace("Attempt to put record: {}", record);
      var topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
      if (getCorruptedRowOffsets(topicPartition).contains(record.kafkaOffset())) {
        errorReport(record);
        continue;
      }

      var topicPartitionWriter = topicPartitionWriters.get(topicPartition);
      topicPartitionWriter.appendRecord(record);
      if (matchRetryBoundary(topicPartition, record)
          || topicPartitionWriter.isExceedRecordLimit()) {
        log.trace("Exceed record limit");
        flushTopicPartitionWriter(topicPartitionWriter, topicPartition);
      }
    }
  }

  private void flushTopicPartitionWriter(
      BigqueryStreamWriter topicPartitionWriter, TopicPartition topicPartition) {
    topicPartitionWriter
        .write()
        .ifPresent(
            appendContext -> {
              var inflights = getInflightContexts(topicPartition);
              inflights.add(appendContext);
            });
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Return immediately here since the task will already be stopped
    if (stopped) {
      log.warn("Flush called after task was stopped. Skipping flush operation.");
      return;
    }

    // Prevent flush execution when no partitions are assigned, such as during rebalancing
    if (closed) {
      log.info("Skipping flush because the task is already closed");
      return;
    }

    currentOffsets.forEach(
        (topicPartition, offsetAndMetadata) ->
            flushTopicPartitionWriter(topicPartitionWriters.get(topicPartition), topicPartition));
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Remove already revoked partitions
    var assigned = topicPartitionWriters.keySet();
    var filteredOffsets =
        currentOffsets.entrySet().stream()
            .filter(e -> assigned.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    flush(filteredOffsets);
    topicPartitionWriters.entrySet().parallelStream()
        .forEach(
            entry -> {
              var topicPartition = entry.getKey();
              var bigqueryStreamWriter = entry.getValue();
              preCommitForTopicPartitionWriter(
                  filteredOffsets, topicPartition, bigqueryStreamWriter);
            });
    log.trace("commit.offsets: {}", filteredOffsets);
    return filteredOffsets;
  }

  private void preCommitForTopicPartitionWriter(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets,
      TopicPartition topicPartition,
      BigqueryStreamWriter bigqueryStreamWriter) {
    bigqueryStreamWriter.waitAllInflightRequests();

    boolean shouldCommit = false;
    var appendContexts = getInflightContexts(topicPartition);
    if (appendContexts.isEmpty()) {
      return;
    }

    var oldRetryBoundaries = getRetryBoundaries(topicPartition);
    var newRetryBoundaries =
        oldRetryBoundaries.tailSet(currentOffsets.get(topicPartition).offset(), false);
    retryBoundaries.put(topicPartition, new TreeSet<>(newRetryBoundaries));
    oldRetryBoundaries.clear();
    for (var appendContext : appendContexts) {
      if (!preCommitForAppendContext(currentOffsets, topicPartition, appendContext)) {
        break;
      }
      shouldCommit = true;
    }

    appendContexts.clear();

    if (shouldCommit) {
      bigqueryStreamWriter.commit();
      log.info("bigquery.commit.success: {}", topicPartition);
    } else {
      bigqueryStreamWriter.reset();
    }
  }

  private boolean preCommitForAppendContext(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets,
      TopicPartition topicPartition,
      AppendContext appendContext) {
    log.debug("PreCommit for AppendContext: {}", appendContext);

    var topicPartitionRetryBoundaries = getRetryBoundaries(topicPartition);
    var corruptedRowOffsets = getCorruptedRowOffsets(topicPartition);
    corruptedRowOffsets.clear();

    if (appendContext.isAlreadyExists()) {
      return true;
    }

    if (appendContext.isOutOfRange()) {
      rewindToAppendContextOffset(currentOffsets, topicPartition, appendContext);
      return false;
    }

    if (appendContext.hasError()) {
      corruptedRowOffsets.addAll(appendContext.corruptedRowKafkaOffsets());
      topicPartitionRetryBoundaries.add(appendContext.getLastKafkaOffset());
      log.error("AppendContext has error", appendContext.getError());
      log.error(
          "PreCommit for AppendContext is failed, a record in corruptedRowOffsets is ignored next retry: {corruptedRowOffsets={}, grpcErrorCode={}}}",
          corruptedRowOffsets,
          appendContext.getGrpcStatusCode());
      if (appendContext.hasUnretryableError()) {
        log.error(
            "Unretryable error is occured: {topic={}, partition={}, from={}, to={}}",
            topicPartition.topic(),
            topicPartition.partition(),
            appendContext.getFirstKafkaOffset(),
            appendContext.getLastKafkaOffset());
      }
      rewindToAppendContextOffset(currentOffsets, topicPartition, appendContext);
      return false;
    }

    return true;
  }

  private void rewindToAppendContextOffset(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets,
      TopicPartition topicPartition,
      AppendContext appendContext) {
    log.info("rewind.offsets: {}", currentOffsets);
    var currentOffsetAndMetadata = currentOffsets.get(topicPartition);
    var newOffsetAndMetaData =
        new OffsetAndMetadata(
            appendContext.getFirstKafkaOffset(),
            currentOffsetAndMetadata.leaderEpoch(),
            currentOffsetAndMetadata.metadata());
    currentOffsets.put(topicPartition, newOffsetAndMetaData);
    context.offset(topicPartition, appendContext.getFirstKafkaOffset());
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    super.close(partitions);
    // NOTE: Handle potential multiple invocations of close() by treating map entries as Optional,
    // since the map may have already been cleared in a prior call.
    partitions.forEach(
        tp ->
            Optional.ofNullable(topicPartitionWriters.get(tp)).ifPresent(writer -> writer.close()));
    topicPartitionWriters.clear();
    closed = true;
    log.trace("task.close: {}", partitions);
  }

  @Override
  public void stop() {
    stopped = true;
    log.trace("task.close");
  }
}
