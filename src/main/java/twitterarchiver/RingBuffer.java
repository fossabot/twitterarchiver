package twitterarchiver;

/**
 * A very simple ring buffer you can use for a running average.
 */
public class RingBuffer {
  private final int ringHash;
  private final int[] ring;
  private int length = 0;

  public RingBuffer(int ringSize) {
    int i = 1;
    while (i < ringSize) {
      i *= 2;
    }
    this.ringHash = i - 1;
    ring = new int[i];
  }

  public void append(int c) {
    ring[length++ & ringHash] = c;
  }

  public void clear() {
    length = 0;
  }

  public int average() {
    int total = 0;
    int num = 0;
    for (int i : ring) {
      total += i;
      if (i != 0) num++;
    }
    return total / num;
  }
}