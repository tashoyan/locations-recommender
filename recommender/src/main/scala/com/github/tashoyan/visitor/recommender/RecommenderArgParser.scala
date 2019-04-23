package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.recommender.RecommenderConfig._
import scopt.OptionParser

trait RecommenderArgParser {

  val parser: OptionParser[RecommenderConfig] = new OptionParser[RecommenderConfig]("recommender") {
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

    opt[Double]("epsilon")
      .optional()
      .valueName("<value>")
      .action((value, conf) => conf.copy(epsilon = value))
      .validate { value =>
        if (value < 0) failure("Epsilon must be non-negative")
        else success
      }
      .text("The recommender stops iterations" +
        " when the euclidean distance between the two vectors calculated on two subsequent iterations" +
        " is equal or below the epsilon value." +
        s" Default value: $defaultEpsilon")

    opt[Int]("max-iterations")
      .optional()
      .valueName("<value>")
      .action((value, conf) => conf.copy(maxIterations = value))
      .validate { value =>
        if (value < 0) failure("Maximum iterations number must be non-negative")
        else success
      }
      .text("The recommender stops iterations" +
        " after reaching the maximum number of iterations" +
        s" Default value: $defaultMaxIterations")

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
