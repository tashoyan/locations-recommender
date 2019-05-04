package com.github.tashoyan.recommender.knn

import scopt.OptionParser

trait SimilarPersonsBuilderArgParser {

  val parser: OptionParser[SimilarPersonsBuilderConfig] = new OptionParser[SimilarPersonsBuilderConfig]("similar-persons-builder") {
    head("Similar Persons Builder")

    opt[String]("data-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(dataDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Data directory must be non-empty path")
        else success
      }
      .text("Data directory to put the generated samples")

    opt[Int]("last-days-count")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(lastDaysCount = value))
      .validate { value =>
        if (value <= 0) failure("Last days count must be positive")
        else success
      }
      .text("Consider past visits occurred during this number of last days")

    opt[Double]("place-weight")
      .required()
      .valueName("<value>")
      .action((value, conf) => conf.copy(placeWeight = value))
      .validate { value =>
        if (value <= 0 || value >= 1) failure("Weight must be in the interval (0; 1)")
        else success
      }
      .text("Weight for place-based person similarity." +
        " Sum of weights for place-based and category-based similarities must be 1.0")

    opt[Double]("category-weight")
      .required()
      .valueName("<value>")
      .action((value, conf) => conf.copy(categoryWeight = value))
      .validate { value =>
        if (value <= 0 || value >= 1) failure("Weight must be in the interval (0; 1)")
        else success
      }
      .text("Weight for category-based person similarity." +
        " Sum of weights for place-based and category-based similarities must be 1.0")

    opt[Int]("k-nearest")
      .required()
      .valueName("<value>")
      .action((value, conf) => conf.copy(kNearest = value))
      .validate { value =>
        if (value <= 0) failure("K nearest must be positive")
        else success
      }
      .text("How many nearest (most similar) persons to consider")

    checkConfig { config =>
      if (config.placeWeight + config.categoryWeight != 1.0)
        failure(
          s"Sum of weights must be 1.0: for place-based similarity: ${config.placeWeight}, for category-based similarity: ${config.categoryWeight}"
        )
      else
        success
    }

    help("help")
    version("version")
  }

}
