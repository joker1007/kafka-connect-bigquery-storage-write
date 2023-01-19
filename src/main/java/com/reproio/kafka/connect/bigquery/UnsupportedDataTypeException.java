package com.reproio.kafka.connect.bigquery;

class UnsupportedDataTypeException extends RuntimeException {

  public UnsupportedDataTypeException(String message) {
    super(message);
  }
}
