#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

samples_dir="samples"

base_dir="$(cd "$(dirname -- "$0")" ; cd .. ; pwd)"

jar_file="$(ls $base_dir/recommender/target/recommender-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

app_name="$(basename $0)"
log_config_file="$base_dir/conf/log4j.xml"
spark_config_file="$base_dir/conf/spark.conf"
spark-submit \
--name "$app_name" \
--properties-file "$spark_config_file" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$log_config_file" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file://$log_config_file" \
--class com.github.tashoyan.visitor.recommender.knn.SimilarPersonsBuilderMain \
"$jar_file" \
--samples-dir "$samples_dir" \
--alpha-place 0.5 \
--alpha-category 0.5
