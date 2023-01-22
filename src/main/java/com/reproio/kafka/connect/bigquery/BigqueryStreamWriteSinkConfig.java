package com.reproio.kafka.connect.bigquery;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class BigqueryStreamWriteSinkConfig extends AbstractConfig {
  public static final String PROJECT_CONFIG = "project";
  private static final ConfigDef.Type PROJECT_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String PROJECT_DOC = "The BigQuery project to write to";

  public static final String DATASET_CONFIG = "dataset";
  private static final ConfigDef.Type DATASET_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance DATASET_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String DATASET_DOC = "The BigQuery dataset to write to";

  public static final String TABLE_CONFIG = "table";
  private static final ConfigDef.Type TABLE_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance TABLE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String TABLE_DOC = "The BigQuery table to write to";

  public static final String KEYFILE_CONFIG = "keyfile";
  private static final ConfigDef.Type KEYFILE_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance KEYFILE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String KEYFILE_DOC = "keyfile for auth";

  public static final String WRITE_MODE_CONFIG = "write_mode";
  private static final ConfigDef.Type WRITE_MODE_TYPE = ConfigDef.Type.STRING;
  private static final ConfigDef.Importance WRITE_MODE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String WRITE_MODE_DOC = "keyfile for auth";

  protected BigqueryStreamWriteSinkConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public BigqueryStreamWriteSinkConfig(Map<?, ?> originals) {
    this(getConfig(), originals);
  }

  public static ConfigDef getConfig() {
    return new ConfigDef()
        .define(PROJECT_CONFIG, PROJECT_TYPE, PROJECT_IMPORTANCE, PROJECT_DOC)
        .define(DATASET_CONFIG, DATASET_TYPE, DATASET_IMPORTANCE, DATASET_DOC)
        .define(TABLE_CONFIG, TABLE_TYPE, TABLE_IMPORTANCE, TABLE_DOC)
        .define(KEYFILE_CONFIG, KEYFILE_TYPE, KEYFILE_IMPORTANCE, KEYFILE_DOC)
        .define(WRITE_MODE_CONFIG, WRITE_MODE_TYPE, WRITE_MODE_IMPORTANCE, WRITE_MODE_DOC);
  }
}
