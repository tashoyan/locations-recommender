package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.knn.KnnRecommenderConfig._
import scopt.OptionParser

trait KnnRecommenderArgParser {

  val parser: OptionParser[KnnRecommenderConfig] = new OptionParser[KnnRecommenderConfig]("recommender") {
    head("Recommender")

    opt[String]("samples-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(samplesDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Samples directory must be non-empty path")
        else success
      }
      .text("Samples directory to put the generated samples")

    opt[Int]("max-recommendations")
      .optional()
      .valueName("<value>")
      .action((value, conf) => conf.copy(maxRecommendations = value))
      .validate { value =>
        if (value < 0) failure("Maximum recommendations number must be non-negative")
        else success
      }
      .text("The recommender provides at most this number of recommendations" +
        s" Default value: $defaultMaxRecommendations")

    help("help")
    version("version")
  }

}