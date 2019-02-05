package org.sonarsource.angel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class ComposerFind {
  public interface ComposerFindOptions extends PipelineOptions {
    @Description("Input path")
    @Required
    String getInput();

    void setInput(String value);

    @Description("Output path")
    @Required
    String getOutput();

    void setOutput(String value);
  }


  private static class ReadRepositoryFn extends DoFn<FileIO.ReadableFile, String> {
    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<String> receiver) {
      String repositoryID = file.getMetadata().resourceId().getFilename();

      try (ReadableByteChannel byteChannel = file.open();
           InputStream stream = Channels.newInputStream(byteChannel);
           BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {

        String line;
        while ((line = br.readLine()) != null) {
          String[] words = line.split("\\s+",-1);
          String fileName = words[words.length-1];
          if (fileName != null) {
            if (fileName.equalsIgnoreCase("composer.json")) {
              receiver.output(repositoryID);
            }
          }
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  static void runComplex(ComposerFindOptions options) {
    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();

    p.apply(FileIO.match().filepattern(input))
            .apply(FileIO.readMatches())
            .apply("ReadFile", ParDo.of(new ReadRepositoryFn()))
            .apply("DistinctRepos",Distinct.create())
            .apply("WriteRepoIds",TextIO.write().to(output+"-composer.txt"));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    ComposerFindOptions options =
            PipelineOptionsFactory.fromArgs(args)
                    .withValidation()
                    .as(ComposerFindOptions.class);

    runComplex(options);
  }


}
