#!/bin/bash

usage() { 
  echo "Usage: "
  echo "      $0 [-h <host>] [-p <port>] [-d <db_name>] [-u <userId>]"
  exit 1; 
}

while getopts ":h:p:d:u:" o; do
    case "${o}" in
        h)
            h=${OPTARG}
            ;;
        p)
            p=${OPTARG}
            ;;
        d)
            d=${OPTARG}
            ;;
        u)
            u=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${h}" ] || [ -z "${p}" ] || [ -z "${d}" ] || [ -z "${u}" ]; then
    usage
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

sbt clean package

$SPARK_HOME/bin/spark-submit \
  --class sample.BusinessRecommendations \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.10:1.1.0 \
  ${DIR}/target/scala-2.10/businessRecommendations_2.10-1.0.jar ${h} ${p} ${d} ${u}
