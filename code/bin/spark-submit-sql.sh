#!/usr/bin/env bash
echo "input options: $@"
i=0
j=0
while [[ $# -gt 0 ]]
do
  key="$1"
  case "$key" in
    -e)
      if [ $# -eq 1 ]; then
        echo "should set -e value"
        exit 1
      fi
      EXTENSION_SQL="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    -f)
      if [ $# -eq 1 ];then
        echo "should set -f value"
        exit 1
      fi
      FILE="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param["$i"]="--files"
      i=`expr $i + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      param["$i"]="$2"
      i=`expr $i + 1`
      shift 2
      ;;
    -o)
      if [ $# -eq 1 ]; then
        echo "should set -o value"
        exit 1
      fi
      OUTPUT="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    -format)
      if [ $# -eq 1 ]; then
        echo "should set -format value"
        exit 1
      fi
      FORMAT="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    -sep)
      if [ $# -eq 1 ]; then
        echo "should set -sep value"
        exit 1
      fi
      SEP="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    -header)
      if [ $# -eq 1 ]; then
        echo "should set -header value"
        exit 1
      fi
      HEADER="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    -colType)
      if [ $# -eq 1 ]; then
        echo "should set -colType value"
        exit 1
      fi
      COLTYPE="$2"
      param_sql["$j"]="$1"
      j=`expr $j + 1`
      param_sql["$j"]="$2"
      j=`expr $j + 1`
      shift 2
      ;;
    --count)
      COUNT="true"
      param_sql["$j"]="-count"
      j=`expr $j + 1`
      param_sql["$j"]="true"
      j=`expr $j + 1`
      shift
      ;;
    *)
      param["$i"]="$key"
      i=`expr $i + 1`
      shift
      ;;
  esac
done
echo -e= "${EXTENSION_SQL}"
echo -f = "${FILE}"
echo -o = "${OUTPUT}"
echo -format = "${FORMAT}"
echo -sep = "${SEP}"
echo -header = "${HEADER}"
echo -colType = "${COLTYPE}"
echo " "
echo "=============== split ================="
echo " "
echo final output param = "${param[@]}"
echo final output param sql = "${param_sql[@]}"

CLASS="com.ushareit.sql.SparkSubmitSql"
JARS_PATH="$(dirname "$0")"/spark-submit-sql-1.0-SNAPSHOT.jar

spark-submit --deploy-mode cluster --class "${CLASS}" "${param[@]}" "${JARS_PATH}" "${param_sql[@]}"