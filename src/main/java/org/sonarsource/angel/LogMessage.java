package org.sonarsource.angel;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

@DefaultCoder(AvroCoder.class)
public class LogMessage {
  @Nullable private String projectName;
  @Nullable private String ruleId;
  @Nullable private int entryPointCount;
  @Nullable private int issueCount;

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
