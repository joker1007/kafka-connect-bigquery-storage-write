package com.reproio.kafka.connect.bigquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.kafka.common.TopicPartition;

public class WriterStreamOffset {
  private final TopicPartition topicPartition;
  private String streamName;
  private int offset;
  private long differenceFromKafkaOffset;

  private final Path offsetDir =
      Paths.get(System.getenv().getOrDefault("WRITER_STREAM_OFFSET_DIR", "/tmp"));

  @SneakyThrows
  public WriterStreamOffset(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
    if (Files.exists(offsetFilePath())) {
      List<String> lines = Files.readAllLines(offsetFilePath());
      this.streamName = lines.get(0).strip();
      this.offset = Integer.parseInt(lines.get(1).strip());
      this.differenceFromKafkaOffset = Integer.parseInt(lines.get(2).strip());
    } else {
      Files.createDirectories(offsetDir);
    }
  }

  public void write() throws IOException {
    Files.write(
        offsetFilePath(),
        List.of(streamName, String.valueOf(offset), String.valueOf(differenceFromKafkaOffset)));
  }

  public void initializeOffset(String streamName, long currentKafkaOffset) {
    this.streamName = streamName;
    this.offset = 0;
    this.differenceFromKafkaOffset = currentKafkaOffset;
  }

  private Path offsetFilePath() {
    return offsetDir.resolve(topicPartition.toString() + "-writer-stream-offset");
  }
}
