#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

data_dir="data"

base_dir="$(cd "$(dirname -- "$0")" ; cd .. ; pwd)"

jar_file="$(ls $base_dir/recommender/target/recommender-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

app_name="$(basename $0)"
conf_dir="$base_dir/conf"
log_config_file="$conf_dir/log4j.xml"
spark_config_file="$conf_dir/spark.conf"
log_dir="$base_dir/log"
spark-submit \
--name "$app_name" \
--properties-file "$spark_config_file" \
--conf "spark.driver.extraJavaOptions=-Dapp.log.dir=$log_dir -Dlog4j.configuration=file://$log_config_file" \
--conf "spark.executor.extraJavaOptions=-Dapp.log.dir=$log_dir -Dlog4j.configuration=file://$log_config_file" \
--class com.github.tashoyan.recommender.knn.SimilarPersonsBuilderMain \
"$jar_file" \
--data-dir "$data_dir" \
--last-days-count 7 \
--place-weight 0.5 \
--category-weight 0.5 \
--k-nearest 1000
