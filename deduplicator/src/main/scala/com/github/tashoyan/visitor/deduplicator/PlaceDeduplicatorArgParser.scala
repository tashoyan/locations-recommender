package com.github.tashoyan.visitor.deduplicator

import com.github.tashoyan.visitor.deduplicator.PlaceDeduplicatorConfig._
import scopt.OptionParser

trait PlaceDeduplicatorArgParser {

  val parser: OptionParser[PlaceDeduplicatorConfig] = new OptionParser[PlaceDeduplicatorConfig]("place-deduplicator") {
    head("Place Deduplicator")

    opt[String]("data-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(dataDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Data directory must be non-empty path")
        else success
      }
      .text("Data directory to put the generated samples")

    opt[Double]("max-place-distance-meters")
      .optional()
      .valueName("<path>")
      .action((value, conf) => conf.copy(maxPlaceDistanceMeters = value))
      .validate { value =>
        if (value < 0) failure("Max distance between places must be non-negative")
        else success
      }
      .text("Max distance in meters between two places when they can be considered as the same place duplicated." +
        s" Default value is $defaultMaxPlaceDistanceMeters")

    opt[Int]("max-name-difference")
      .optional()
      .valueName("<path>")
      .action((value, conf) => conf.copy(maxNameDifference = value))
      .validate { value =>
        if (value < 0) failure("Max difference between names must be non-negative")
        else success
      }
      .text("Max difference between names of two places when they can be considered as the same place duplicated." +
        s" Default value is $defaultMaxNameDifference")

    help("help")
    version("version")
  }

}
