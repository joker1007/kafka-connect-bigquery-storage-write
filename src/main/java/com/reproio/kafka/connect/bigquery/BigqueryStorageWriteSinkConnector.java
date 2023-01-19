package com.reproio.kafka.connect.bigquery;

import com.reproio.kafka.connect.bigquery.utils.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

@Slf4j
public class BigqueryStorageWriteSinkConnector extends SinkConnector {
  private Map<String, String> props;

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return BigqueryStorageWriteSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    for (var i = 0; i < maxTasks; i++) {
      configs.add(props);
    }
    return configs;
  }

  @Override
  public void stop() {
    log.trace("connector.stop");
  }

  @Override
  public ConfigDef config() {
    return BigqueryStreamWriteSinkConfig.getConfig();
  }

  @Override
  public String version() {
    return Version.version();
  }
}
