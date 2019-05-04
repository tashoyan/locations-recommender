package com.github.tashoyan.recommender.sample

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

    opt[Long]("place-count")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(placeCount = value))
      .validate { value =>
        if (value <= 0) failure("Place count must be positive")
        else success
      }
      .text("How many places to generate for all regions")

    opt[Long]("person-count")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(personCount = value))
      .validate { value =>
        if (value <= 0) failure("Person count must be positive")
        else success
      }
      .text("How many persons to generate for all regions")

    help("help")
    version("version")
  }

}
