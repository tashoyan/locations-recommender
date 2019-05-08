# Visit Recommender

Recommender system that suggests people what places to visit.

Two implementations are currently available:
- stochastic graph (SG)
- K nearest neighbors (KNN)

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
The vector `x` is the stationary probability vectorof size `N`,
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
To retrieve possible identifiers of persons and regions,
use Spark shell and query the generated samples:
`data/persons_sample` and `data/place_samples`.

Note that the stochastic graph builder does not create
a single huge graph for all persons and all places in all regions.
Instead, it creates graphs for each region and graphs for each pair of regions.
This is done for better scalability.
Per-region graphs are used to recommend places within a person's home region.
In addition, pairwise region graphs are used to recommend places in a non-home region.
