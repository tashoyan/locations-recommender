package com.github.tashoyan.visitor.recommender

import scopt.OptionParser

trait StochasticGraphBuilderArgParser {

  val parser: OptionParser[StochasticGraphBuilderConfig] = new OptionParser[StochasticGraphBuilderConfig]("stochastic-graph-builder") {
    head("Stochastic Graph Builder")

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
