package org.sonarsource.angel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Filter;

public class BasicIndexer {

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

  public static void main(String[] args) {
    BasicIndexerOptions options =
      PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .as(BasicIndexerOptions.class);

    Pipeline p = Pipeline.create(options);

    String input = options.getInput();
    String output = options.getOutput();

    p.apply(TextIO.read().from(input))
            .apply(Filter.by((String line) -> line.endsWith("pom.xml")))
            .apply(TextIO.write().to(output));

    p.run().waitUntilFinish();
  }
}
