package com.reproio.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;

@Tag("integration")
class BigqueryStreamWriterIntegrationTest {
  @Container
  BigQueryEmulatorContainer container =
      new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.6.6");

  private final Schema valueSchema =
      SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("int_value", Schema.INT64_SCHEMA)
          .build();

  private int sinkRecordOffset = 0;

  private static final String DATASET_ID = "test_dataset";
  private static final String TABLE_NAME = "bq_table";

  private BigQuery bigQuery;

  @BeforeEach
  void setUp() {
    container.start();
    bigQuery = getBigQueryClient();
    createTable();
  }

  @Test
  void testCommittedMode() throws IOException, InterruptedException {
    container.start();

    var writeClient = getBigQueryWriteClient();
    var writer =
        new BigqueryStreamWriter(
            container.getProjectId(),
            DATASET_ID,
            TABLE_NAME,
            BigqueryStreamWriter.WriteMode.COMMITTED,
            1000,
            writeClient);

    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());

    Optional<BigqueryStreamWriter.AppendContext> appendContext = writer.write();
    assertTrue(appendContext.isPresent());
    writer.waitAllInflightRequests();
    assertAppendContext(appendContext.get(), 0, List.of(0L, 1L), List.of());
    assertEquals(0, writer.currentBufferChunk.size());

    var result = queryTable();
    assertEquals(2, result.getTotalRows());
  }

  @Test
  void testPendingMode() throws IOException, InterruptedException {
    container.start();

    var writeClient = getBigQueryWriteClient();
    var writer =
        new BigqueryStreamWriter(
            container.getProjectId(),
            DATASET_ID,
            TABLE_NAME,
            BigqueryStreamWriter.WriteMode.PENDING,
            1000,
            writeClient);

    writer.appendRecord(buildSinkRecord());
    writer.appendRecord(buildSinkRecord());

    Optional<BigqueryStreamWriter.AppendContext> appendContext = writer.write();
    assertTrue(appendContext.isPresent());
    writer.waitAllInflightRequests();
    assertAppendContext(appendContext.get(), 0, List.of(0L, 1L), List.of());
    assertEquals(0, writer.currentBufferChunk.size());

    var result = queryTable();
    assertEquals(0, result.getTotalRows());

    writer.commit();

    result = queryTable();
    assertEquals(2, result.getTotalRows());
  }

  private TableResult queryTable() throws InterruptedException {
    return bigQuery.query(
        QueryJobConfiguration.newBuilder("SELECT * FROM " + DATASET_ID + "." + TABLE_NAME).build());
  }

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

  private void assertAppendContext(
      BigqueryStreamWriter.AppendContext appendContext,
      int expectedStreamOffset,
      List<Long> expectedWrittenRecordKafkaOffsets,
      List<Long> expectedErrorRecordKafkaOffsets) {

    assertNotNull(appendContext.getAppendRowsResponse());
    assertEquals(expectedStreamOffset, appendContext.getWrittenWriterStreamOffset());
    assertEquals(expectedWrittenRecordKafkaOffsets, appendContext.getWrittenRecordKafkaOffsets());
    assertEquals(expectedErrorRecordKafkaOffsets, appendContext.getErrorRecordKafkaOffsets());
  }

  private BigQuery getBigQueryClient() {
    String url = container.getEmulatorHttpEndpoint();
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId(container.getProjectId())
            .setHost(url)
            .setLocation(url)
            .setCredentials(NoCredentials.getInstance())
            .build();
    return options.getService();
  }

  private void createTable() {
    bigQuery.create(DatasetInfo.newBuilder(DATASET_ID).build());
    bigQuery.create(
        TableInfo.newBuilder(
                TableId.of(DATASET_ID, TABLE_NAME),
                StandardTableDefinition.of(
                    com.google.cloud.bigquery.Schema.of(
                        Field.of("id", StandardSQLTypeName.STRING),
                        Field.of("int_value", StandardSQLTypeName.INT64))))
            .build());
  }

  private BigQueryWriteClient getBigQueryWriteClient() throws IOException {
    var writeSettings =
        BigQueryWriteSettings.newBuilder()
            .setEndpoint(container.getHost() + ":" + container.getMappedPort(9060))
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(
                BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                    .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                    .build())
            .build();
    var writeClient = BigQueryWriteClient.create(writeSettings);
    return writeClient;
  }
}
