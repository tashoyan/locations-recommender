# Visit Recommender

Recommender system that suggests people what places to visit.

[![Build Status](https://travis-ci.com/tashoyan/visit-recommender.svg?branch=master)](https://travis-ci.com/tashoyan/visit-recommender)

Two implementations are currently available:
- stochastic graph (SG)
- K nearest neighbors (KNN)

## Building the code

Use Maven to build the code:
```text
mvn clean install
```

## Generating samples

Both Visit Recommender implementations require the following data:
- places to visit
- categories of places
- persons
- facts of location visits by persons

To generate simulated data, run the following command:
```bash
./bin/sample_generator.sh
```
To customize the generated samples, edit the shell launcher.

## Recommender based on stochastic graph

Stochastic graph is a graph with edges forming a [stochastic matrix](https://en.wikipedia.org/wiki/Stochastic_matrix).
Each edge has a probability value (weight) associated with it.
For each vertex, the sum of probabilities for all outbound edges is `1`.
The SG-based recommender exploits the fact that a stochastic matrix has a stationary probability vector:
```text
x = P * x
```
Here `P` is the stochastic matrix of size `N x N`,
with `P(i, j)` element denoting the probability of moving from vertex `i` to vertex `j`.
This matrix satisfies the stochastic condition:
```text
P(i, 1) + P(i, 2) + ... + P(i, N) = 1 for any vertex i
```
The vector `x` is the stationary probability vector of size `N`,
with `x(j)` element denoting the probability to come to vertex `j`.

The SG-based recommender consists of two components: stochastic graph builder and the recommender itself.

### Stochastic graph builder

To build the stochastic graph of visits, run the command:
```bash
./bin/stochastic_graph_builder.sh
```
The resulting graph will have the following edges:

- *person - (likes) -> place*
  
  Edge weight: how many times the person visited this place.

- *person - (likes) -> category*

  Edge weight: how many times the person visited this place category.

- *place - (is similar to) -> place*

  Edge weight: how many persons visited both places within a short time interval (one week).

- *category - (selected by a person) -> place*

  Edge weight: when visiting a place of some category,
  how many times a person select this particular place.

All weights are normalized to be in the interval `[0; 1]`.

To ensure that the stochastic property is met,
the graph builder applies `beta` balancing factors
to weights of edges *person - (likes) -> place* and *person - (likes) -> category*.
Thus, for each person the sum of weights of all outbound edges is `1`.

To customize `beta` balancing factors and other graph builder parameters,
edit the shell launcher of the graph builder.

### SG recommender

To run the SG recommender, execute the command:
```bash
./bin/stochastic_recommender.sh
```
When asked, enter the person identifier that you want to provide with recommendations.
Optionally, enter the identifier of the region where recommend the places to visit.
If not provided, the home region of the person is used by default.
Then hit Enter to start computations:
```text
(CTRL-C for exit) <person ID>[ <region ID>]: 1060040 0
```
The recommender will output the places recommended to this person
in the decreasing order of probability.

To retrieve possible identifiers of persons and regions,
use Spark shell and query the generated samples:
`data/persons_sample` and `data/place_samples`.

Note that the stochastic graph builder does not create
a single huge graph for all persons and all places in all regions.
Instead, it creates graphs for each region and graphs for each pair of regions.
This is done for better scalability.
Per-region graphs are used to recommend places within a person's home region.
In addition, pairwise region graphs are used to recommend places in a non-home region.

## Recommender based on K nearest neighbors

[KNN algorithm](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm) estimates
an unknown feature value as an average over K nearest points with known feature values.
KNN-based recommender builds two feature vectors (rating vectors) for each person:

- place ratings vector: x(i), where x(i) is the rating given by the person to the place i
- category ratings vector: x(i), where x(i) is the rating given by the person to the place category i

Place or category rating is the number of visits made by this person during some time interval (e.g. one week).
Note that rating vectors are very sparse, because a person visits only a small subset of all possible places.

The distance between two persons `p1` and `p2` is expressed
in terms of [cosine similarity](https://en.wikipedia.org/wiki/Cosine_similarity)
between their respective rating vectors:
```text
similarity(p1, p2) = place_weight * place_similarity(p1, p2) + category_weight * category_similarity(p1, p2)
```
Here `place_similarity(p1, p2)` and `category_similarity(p1, p2)` are the cosine similarities
between place and category rating vectors of the two persons.

As the name of the algorithm suggests, only `K` most similar persons are processed for a target person.

For a target person `p`, the estimated rating of a place `i` is calculated according to the formula:
```text
estimated_rating(p, i) = [rating(p1, i) * similarity(p, p1) + ... + rating(pK, i) * similarity(p, pK)] / [similarity(p, p1) + ... + similarity(p, pK)]
```

The KNN-based recommender consists of two components: rating vectors builder and the recommender itself.

### Rating vectors builder

To build the rating vectors, run the command:
```bash
./bin/rating_vectors_builder.sh
```

### KNN recommender

To run the KNN recommender, execute the command:
```bash
./bin/knn_recommender.sh
```
The user interface is pretty much the same as for the SG recommender:
```text
(CTRL-C for exit) <person ID>[ <region ID>]: 1060040 0
```
The recommender will output the places recommended to this person
in the decreasing order of estimated rating.

Similarly to SG-based recommender,
rating vectors are generated separately for each region and for each pair of regions.
The idea is the same - avoid huge data sets and improve scalability.

## Place deduplicator

*Not completed yet*

The data set of places is supposed to be user-generated content.
Persons may enter new places they have just visited.
Such content may contain user mistakes, for example:

- typos in place names
- imprecise place coordinates

Place Deduplicator is a tool to detect and resolve place duplicates.
It uses the following algorithm: if two places have close locations and similar names,
then they are marked as a duplicate of the same place.
To check if the two places have close locations,
Place Deduplicator calculates the distance between them
and compares it with a threshold.
To check if the two places have similar names,
the tool calculates the [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance)
between their names and compares with a threshold.
