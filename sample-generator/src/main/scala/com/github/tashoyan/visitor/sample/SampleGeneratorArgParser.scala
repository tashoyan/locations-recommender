package com.github.tashoyan.visitor.sample

import scopt.OptionParser

trait SampleGeneratorArgParser {

  val parser: OptionParser[SampleGeneratorConfig] = new OptionParser[SampleGeneratorConfig]("sample-generator") {
    head("Sample Generator")

    opt[String]("data-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(dataDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Data directory must be non-empty path")
        else success
      }
      .text("Data directory to put the generated samples")

    help("help")
    version("version")
  }

}
