
// get the Elastic Search image
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.6.1

// start Elastic Search container
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.1

// check container is running ok
curl -X GET "localhost:9200/_cat/nodes?v&pretty"

// bulk index, movies.json can be generated via any scripts of (Go, JavaScript, Python)
curl -X POST "localhost:9200/movie/_bulk" -H 'Content-Type: application/json' --data-binary @movies.json
