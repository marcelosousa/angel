package org.sonarsource.angel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

public class ProjectLanguageIndexer {
  public interface ProjectLanguageIndexerOptions extends PipelineOptions {
    @Description("Input path")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Output path")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  private static class ReadRepositoryFn extends DoFn<FileIO.ReadableFile, KV<String, Long>> {
    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, Long>> receiver) {
      String repositoryID = file.getMetadata().resourceId().getFilename();

      try (ReadableByteChannel byteChannel = file.open();
           InputStream stream = Channels.newInputStream(byteChannel);
           BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {

        String line;
        Long fileCount = new Long(0);
        while ((line = br.readLine()) != null) {
          String[] words = line.split("\\s+",-1);
          String fileName = words[words.length-1];
          if (fileName != null) {
            if (fileName.endsWith(".cs")) {
              fileCount++;
            }
          }
        }
        if (fileCount > 0) {
          receiver.output(KV.of(repositoryID, fileCount));
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  private static void runJob(ProjectLanguageIndexerOptions options) {
    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();

    p.apply(FileIO.match().filepattern(input))
      .apply(FileIO.readMatches())
      .apply("ReadFile", ParDo.of(new ProjectLanguageIndexer.ReadRepositoryFn()))
      .apply("FormatOutput", MapElements.into(TypeDescriptors.strings())
      .via(item -> item.getKey() + ": " + item.getValue()))
      .apply(TextIO.write().to(output));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    ProjectLanguageIndexerOptions options =
            PipelineOptionsFactory.fromArgs(args)
                    .withValidation()
                    .as(ProjectLanguageIndexerOptions.class);

    runJob(options);
  }
}
