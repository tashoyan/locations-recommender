package com.github.tashoyan.recommender.knn

import scopt.OptionParser

trait RatingVectorsBuilderArgParser {

  val parser: OptionParser[RatingVectorsBuilderConfig] = new OptionParser[RatingVectorsBuilderConfig](getClass.getSimpleName) {
    head(getClass.getSimpleName)

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

    opt[Int]("max-rated-places")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(maxRatedPlaces = value))
      .validate { value =>
        if (value <= 0) failure("Max rated places must be positive")
        else success
      }
      .text("For each person consider at most this number of visited places with highest visit counts")

    opt[Int]("max-rated-categories")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(maxRatedCategories = value))
      .validate { value =>
        if (value <= 0) failure("Max rated categories must be positive")
        else success
      }
      .text("For each person consider at most this number of visited place categories with highest visit counts")

    help("help")
    version("version")
  }

}
