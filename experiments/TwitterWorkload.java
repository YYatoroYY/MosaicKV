package ec.trace;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import site.ycsb.WorkloadException;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.workloads.CoreWorkload;

/** Keep CoreWorkload key/operation generation; supply project-specific lengths. */
public final class TwitterWorkload extends CoreWorkload {
  @Override public void init(Properties properties) throws WorkloadException {
    super.init(properties);
    if ("run".equals(properties.getProperty("trace.phase"))) {
      try {
        fieldlengthgenerator = new CsvLengths(properties.getProperty("trace.histogram"));
      } catch (IOException | IllegalArgumentException e) {
        throw new WorkloadException("Invalid Twitter length histogram", e);
      }
    }
  }

  private static final class CsvLengths extends NumberGenerator {
    private final long[] cumulative = new long[4096];
    private long total;
    private double weighted;

    CsvLengths(String filename) throws IOException {
      try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
        if (!"new_size_bytes,requests".equals(reader.readLine())) {
          throw new IllegalArgumentException("Expected new_size_bytes,requests header");
        }
        for (int i = 0; i < cumulative.length; i++) {
          String line = reader.readLine();
          if (line == null) { throw new IllegalArgumentException("Missing histogram row"); }
          String[] fields = line.split(",");
          if (fields.length != 2 || Integer.parseInt(fields[0]) != i + 1) {
            throw new IllegalArgumentException("Expected ordered sizes 1..4096");
          }
          long count = Long.parseLong(fields[1]);
          if (count < 0 || count > Long.MAX_VALUE - total) {
            throw new IllegalArgumentException("Invalid histogram count");
          }
          total += count;
          cumulative[i] = total;
          weighted += (i + 1) * (double) count;
        }
        if (reader.readLine() != null || total == 0) {
          throw new IllegalArgumentException("Invalid histogram length or total");
        }
      }
    }

    @Override public Long nextValue() {
      long draw = ThreadLocalRandom.current().nextLong(total);
      int lo = 0, hi = cumulative.length - 1;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (draw < cumulative[mid]) { hi = mid; } else { lo = mid + 1; }
      }
      long value = lo + 1L;
      setLastValue(value);
      return value;
    }
    @Override public double mean() { return weighted / total; }
  }
}
