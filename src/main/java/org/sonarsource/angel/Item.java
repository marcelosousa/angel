package org.sonarsource.angel;

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
