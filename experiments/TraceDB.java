package ec.trace;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/** Offline sink: YCSB itself chooses keys, operations and value lengths. */
public final class TraceDB extends DB {
  private BufferedWriter writer;
  private String phase;

  @Override public void init() throws DBException {
    if (!"1".equals(getProperties().getProperty("threadcount"))) {
      throw new DBException("TraceDB requires exactly one generator thread");
    }
    phase = getProperties().getProperty("trace.phase");
    try {
      writer = Files.newBufferedWriter(Paths.get(getProperties().getProperty("trace.output")),
          StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    } catch (IOException e) { throw new DBException(e); }
  }

  private Status emit(String kind, String key, Map<String, ByteIterator> values) {
    if (!kind.equals(phase) || !key.matches("user[0-9]+") || values.size() != 1) {
      throw new IllegalArgumentException("Expected one field and a load INSERT or run UPDATE");
    }
    long size = values.values().iterator().next().bytesLeft();
    if (size < 1 || size > 16777216) { throw new IllegalArgumentException("Invalid size"); }
    try {
      writer.write(key); writer.write('\t'); writer.write(Long.toString(size)); writer.newLine();
    } catch (IOException e) { throw new IllegalStateException(e); }
    return Status.OK;
  }

  @Override public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return emit("load", key, values);
  }
  @Override public Status update(String table, String key, Map<String, ByteIterator> values) {
    return emit("run", key, values);
  }
  @Override public void cleanup() throws DBException {
    try { if (writer != null) { writer.close(); } }
    catch (IOException e) { throw new DBException(e); }
  }
  @Override public Status read(String t, String k, Set<String> f, Map<String, ByteIterator> r) {
    throw new UnsupportedOperationException("This exporter is update-only");
  }
  @Override public Status scan(String t, String k, int n, Set<String> f,
      Vector<HashMap<String, ByteIterator>> r) {
    throw new UnsupportedOperationException("SCAN is not supported");
  }
  @Override public Status delete(String t, String k) {
    throw new UnsupportedOperationException("DELETE is not supported");
  }
}
