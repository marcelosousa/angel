package org.sonarsource.angel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class BasicIndexer {
  final static TupleTag<String> pom = new TupleTag<String>(){};
  final static TupleTag<String> gradle = new TupleTag<String>(){};

  public interface BasicIndexerOptions extends PipelineOptions {
    @Description("Input path")
    @Required
    String getInput();

    void setInput(String value);

    @Description("Output path")
    @Required
    String getOutput();

    void setOutput(String value);
  }

  private static class ReadFileFn extends DoFn<FileIO.ReadableFile, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, String>> receiver) {
        String repositoryID = file.getMetadata().resourceId().getFilename();

      try (ReadableByteChannel byteChannel = file.open();
           InputStream stream = Channels.newInputStream(byteChannel);
           BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {

        String line;
        while ((line = br.readLine()) != null) {
          String[] words = line.split("\\s+",-1);
          String fileName = words[words.length-1];
          if (fileName != null) {
            if (fileName.equalsIgnoreCase("pom.xml")) {
              receiver.output(KV.of(fileName, repositoryID));
            }
          }
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  private static class ReadRepositoryFn extends DoFn<FileIO.ReadableFile, String> {
    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile file, MultiOutputReceiver receiver) {
      String repositoryID = file.getMetadata().resourceId().getFilename();

      try (ReadableByteChannel byteChannel = file.open();
           InputStream stream = Channels.newInputStream(byteChannel);
           BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {

        String line;
        while ((line = br.readLine()) != null) {
          String[] words = line.split("\\s+",-1);
          String fileName = words[words.length-1];
          if (fileName != null) {
            if (fileName.equalsIgnoreCase("pom.xml")) {
              receiver.get(pom).output(repositoryID);
            }
            if (fileName.equalsIgnoreCase("build.gradle")) {
              receiver.get(gradle).output(repositoryID);
            }
          }
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  static void runSimple(BasicIndexerOptions options) {
    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();

    p.apply(FileIO.match().filepattern(input))
            .apply(FileIO.readMatches())
            .apply("ReadFile", ParDo.of(new ReadFileFn()))
            .apply(Distinct.create())
            .apply("CombineKeys", GroupByKey.create())
            .apply("FormatOutput", MapElements.into(TypeDescriptors.strings())
                    .via(item -> item.getKey() + ": " + item.getValue()))
            .apply(TextIO.write().to(output));

    p.run().waitUntilFinish();
  }

  static void runComplex(BasicIndexerOptions options) {
    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();
    
    PCollectionTuple repoLists = p.apply(FileIO.match().filepattern(input))
            .apply(FileIO.readMatches())
            .apply("ReadFile", ParDo.of(new ReadRepositoryFn()).withOutputTags(pom, TupleTagList.of(gradle)));

    PCollection<String> pomList = repoLists.get(pom);
    PCollection<String> gradleList = repoLists.get(gradle);

    pomList.apply("DistinctPOM",Distinct.create())
           .apply("WritePOM",TextIO.write().to(output+"-pom.txt"));

    gradleList.apply("DistinctGradle",Distinct.create())
              .apply("WriteGradle",TextIO.write().to(output+"-gradle.txt"));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    BasicIndexerOptions options =
      PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .as(BasicIndexerOptions.class);

    runComplex(options);
  }


}
