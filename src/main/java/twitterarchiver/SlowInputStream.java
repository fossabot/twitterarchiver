package twitterarchiver;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class SlowInputStream extends BufferedInputStream {
  private final long start;
  private final double rate;
  private long read;

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    maybeWait();
    int bytes = super.read(b, off, len);
    read += bytes;
    return bytes;
  }

  @Override
  public synchronized int read() throws IOException {
    maybeWait();
    read++;
    return super.read();
  }

  private void maybeWait() {
    double millis = read / rate - (System.currentTimeMillis() - start);
    if (millis > 0) {
      try {
        Thread.sleep((long) millis);
      } catch (InterruptedException e) {
        // Continue
      }
    }
  }

  public SlowInputStream(InputStream in, long length, int time, TimeUnit timeUnit) {
    super(in);
    rate = ((double) length)  / timeUnit.toMillis(time);
    start = System.currentTimeMillis();
  }

  public static void main(String[] args) throws IOException {
    File file = new File("pom.xml");
    SlowInputStream sis = new SlowInputStream(new FileInputStream(file), file.length(), 1, TimeUnit.SECONDS);
    long start = System.currentTimeMillis();
    while (sis.read() != -1);
    System.out.println(System.currentTimeMillis() - start);
  }
}
