package com.reproio.kafka.connect.bigquery.utils;

public class Version {
  private static String version = "unknown";

  static {
    String implementationVersion = Version.class.getPackage().getImplementationVersion();
    if (implementationVersion != null) {
      version = implementationVersion;
    }
  }

  public static String version() {
    return version;
  }
}
