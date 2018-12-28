package org.sonarsource.angel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FileFormatIndexer {

  public interface FileFormatIndexerOptions extends PipelineOptions {
    @Description("Input path")
    @Required
    String getInput();

    void setInput(String value);

    @Description("Output path")
    @Required
    String getOutput();

    void setOutput(String value);
  }

  public static class ReadFileNameFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> receiver) {
      String[] words = line.split("\\s+",-1);
      String fileName = words[words.length-1];
      if (fileName != null) {
        int i = fileName.lastIndexOf('.');
        if (i > 0) {
          receiver.output(fileName.substring(i+1));
        }
      }
    }
  }

  public static class CountFileFormat extends
          PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
      // Get the file names
      PCollection<String> fileNames = lines.apply("GetFileNames", ParDo.of(new ReadFileNameFn()));

      PCollection<KV<String, Long>> languageCount = fileNames.apply(Count.perElement());

      return languageCount;
    }
  }
  static void runJob(FileFormatIndexerOptions options) {
    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();
    p.apply("ReadInput", TextIO.read().from(input))
      .apply("CountFileFormat", new CountFileFormat())
      .apply("FilterSmall", Filter.by(k -> k.getValue() > 4))
      .apply("FormatOutput", MapElements.into(TypeDescriptors.strings())
      .via(item -> item.getKey() + ": " + item.getValue()))
      .apply(TextIO.write().to(output));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    FileFormatIndexerOptions options =
            PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .as(FileFormatIndexerOptions.class);

    runJob(options);
  }
}
