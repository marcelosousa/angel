package org.sonarsource.angel;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class LogMessage {
  private String projectName;
  private String ruleId;
  private int entryPointCount;
  private int issueCount;

  @SuppressWarnings("unused")
  public LogMessage() {}

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
