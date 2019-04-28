package com.github.tashoyan.visitor.recommender.knn

import scopt.OptionParser

trait SimilarPersonsBuilderArgParser {

  val parser: OptionParser[SimilarPersonsBuilderConfig] = new OptionParser[SimilarPersonsBuilderConfig]("similar-persons-builder") {
    head("Similar Persons Builder")

    opt[String]("samples-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(samplesDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Samples directory must be non-empty path")
        else success
      }
      .text("Samples directory to put the generated samples")

    opt[Double]("alpha-place")
      .required()
      .valueName("<value>")
      .action((value, conf) => conf.copy(alphaPlace = value))
      .validate { value =>
        if (value <= 0 || value >= 1) failure("Coefficient must be in the interval (0; 1)")
        else success
      }
      .text("Coefficient for place-based person similarity." +
        " Sum of place-based and category-based coefficients must be 1.0")

    opt[Double]("alpha-category")
      .required()
      .valueName("<value>")
      .action((value, conf) => conf.copy(alphaCategory = value))
      .validate { value =>
        if (value <= 0 || value >= 1) failure("Coefficient must be in the interval (0; 1)")
        else success
      }
      .text("Coefficient for category-based person similarity." +
        " Sum of place-based and category-based coefficients must be 1.0")

    checkConfig { config =>
      if (config.alphaPlace + config.alphaCategory != 1.0)
        failure(
          s"Sum of coefficients must be 1.0: for place-based similarity: ${config.alphaPlace}, for category-based similarity: ${config.alphaCategory}"
        )
      else
        success
    }

    help("help")
    version("version")
  }

}
