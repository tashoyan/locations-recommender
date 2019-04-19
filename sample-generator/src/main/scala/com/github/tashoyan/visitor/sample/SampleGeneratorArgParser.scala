package com.github.tashoyan.visitor.sample

import scopt.OptionParser

trait SampleGeneratorArgParser {

  val parser: OptionParser[SampleGeneratorConfig] = new OptionParser[SampleGeneratorConfig]("sample-generator") {
    head("Sample Generator")

    opt[String]("samples-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(samplesDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Samples directory must be non-empty path")
        else success
      }
      .text("Samples directory to put the generated samples")

    help("help")
    version("version")
  }

}
