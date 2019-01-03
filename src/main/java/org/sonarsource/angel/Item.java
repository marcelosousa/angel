package org.sonarsource.angel;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Item {
  private String repositoryName;
  private int fileCount;

  public Item(String repositoryName, int fileCount) {
    this.repositoryName = repositoryName;
    this.fileCount = fileCount;
  }

  public String getRepositoryName() {
    return repositoryName;
  }

  public int getFileCount() {
    return fileCount;
  }
}
