package org.sonarsource.angel;

public class LogMessage {
  private String projectName;
  private String ruleId;
  private int entryPointCount;
  private int issueCount;

  public LogMessage(String projectName, String ruleId, int entryPointCount, int issueCount) {
    this.projectName = projectName;
    this.ruleId = ruleId;
    this.entryPointCount = entryPointCount;
    this.issueCount = issueCount;
  }

  public String getProjectName() {
    return projectName;
  }

  public String getRuleId() {
    return ruleId;
  }

  public int getEntryPointCount() {
    return entryPointCount;
  }

  public int getIssueCount() {
    return issueCount;
  }
}
