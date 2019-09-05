BASE_DIR=/user/vagrant/hive/stackoverflow

dirs=("users" "posts" "comments" "tags" "votes" "badges" "postlinks")
for i in "${dirs[@]}"
do
    hadoop fs -mkdir -p ${BASE_DIR}/${i}
    hadoop fs -mv output/${i}*.avro ${BASE_DIR}/${i}/
done

dirs=("posttypes" "linktypes" "votetypes")
for i in "${dirs[@]}"
do
    hadoop fs -mkdir -p ${BASE_DIR}/${i}
    hadoop fs -put data/${i}.csv ${BASE_DIR}/${i}/
done

hive -f create_schema_avro.hql
