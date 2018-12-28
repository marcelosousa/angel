package org.sonarsource.angel;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogProcessor {
  public static class EmitLogMessage extends DoFn<FileIO.ReadableFile, LogMessage> {
    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<LogMessage> receiver) {
      String projectName = file.getMetadata().resourceId().getFilename();
      String ruleId = "";
      int entryPointCount = 0;
      int issueCount = 0;
      boolean isParsingRule = false;

      try (ReadableByteChannel byteChannel = file.open();
           InputStream stream = Channels.newInputStream(byteChannel);
           BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
        String line;
        while ((line = br.readLine()) != null) {
          if (!isParsingRule) {
            Pattern p = Pattern.compile("\\[INFO] rule: (?<ruleId>\\w*), entrypoints: (?<entryPoints>\\d*)");
            Matcher m = p.matcher(line);
            if (m.find()) {
              ruleId = m.group("ruleId");
              entryPointCount = Integer.valueOf(m.group("entryPoints"));
              isParsingRule = true;
            }
          } else {
            Pattern p = Pattern.compile(".*has issues");
            Matcher m = p.matcher(line);
            if (m.find()) {
              issueCount = 1;
              receiver.output(new LogMessage(projectName, ruleId, entryPointCount, issueCount));
              ruleId = "";
              entryPointCount = 0;
              isParsingRule = false;
            }
          }
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  public static class EmitTableRow extends DoFn<LogMessage, TableRow> {
    @ProcessElement
    public void processElement(@Element LogMessage msg, OutputReceiver<TableRow> receiver) {
      TableRow row = new TableRow()
        .set("projectName", msg.getProjectName())
        .set("ruleId", msg.getRuleId())
        .set("entryPointCount", msg.getEntryPointCount())
        .set("issueCount", msg.getIssueCount());
    }
  }

  public interface LogProcessorOptions extends PipelineOptions {
    @Description("Log input")
    @Required
    String getLogSource();
    void setLogSource(String logSource);

    @Description("BigQuery table name")
    @Required
    String getLogsTableName();
    void setLogsTableName(String logsTableName);
  }

  private static void runJob(LogProcessorOptions options) {
    Pipeline p = Pipeline.create(options);

    String logSource = options.getLogSource();
    PCollection<FileIO.ReadableFile> logs = p.apply(FileIO.match().filepattern(logSource))
            .apply(FileIO.readMatches());

    PCollection<LogMessage> logMessages = logs
            .apply("LogToMessage", ParDo.of(new EmitLogMessage()));

    PCollection<TableRow> msgAsTableRows = logMessages
            .apply("MessageToRow", ParDo.of(new EmitTableRow()));

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("projectName").setType("STRING"));
    fields.add(new TableFieldSchema().setName("ruleId").setType("STRING"));
    fields.add(new TableFieldSchema().setName("entryPointCount").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("issueCount").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    msgAsTableRows.apply("writeToBigQuery",BigQueryIO.writeTableRows()
            .to(options.getLogsTableName())
            .withSchema(schema)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(LogProcessorOptions.class);
    LogProcessorOptions options =
            PipelineOptionsFactory.fromArgs(args)
                    .withValidation()
                    .as(LogProcessorOptions.class);

    runJob(options);
  }
}
