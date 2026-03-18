# Hadoop Inverted Index

---

## Overview

This project builds an **Inverted Index** using Hadoop MapReduce on a multi-node Docker cluster. It processes three classic books from Project Gutenberg, filters out common stop words, and outputs a searchable index mapping every meaningful word to the files and **line numbers** where it appears.

---

## Cluster Setup

4 Docker containers on a shared network:

| Container | Role |
|-----------|------|
| `namenode` | HDFS filesystem manager (port 9870) |
| `datanode1` | HDFS data storage |
| `resourcemanager` | YARN job scheduler (port 8088) |
| `nodemanager` | Executes MapReduce tasks |

```bash
docker compose up -d
docker exec project1-namenode-1 hdfs dfsadmin -report
```

---

## How It Works

**Mapper** — reads each line, increments a line counter, tokenizes the line, skips stop words, and emits `word → filename:lineNumber`.

**Stop Words** — common words like `the`, `and`, `is` are loaded from `stopwords.txt` via Hadoop's Distributed Cache and filtered out on every node.

**Reducer** — groups all locations per word, deduplicates them, and writes the final index:
```
word    file1.txt:100, file1.txt:450, file2.txt:230
```

---

## Running

```bash
# Start the cluster
docker compose up -d
docker exec -it --user root project1-namenode-1 bash

# Optional: Install Java if not already present
sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*.repo
sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
yum install -y java-1.8.0-openjdk-devel

# Compile and package
javac -classpath $(hadoop classpath) -d /src /src/InvertedIndex.java
jar -cvf /src/invertedindex.jar -C /src .

# Upload to HDFS
hdfs dfs -mkdir -p /input
hdfs dfs -put /books/*.txt /input/
hdfs dfs -put /src/stopwords.txt /stopwords.txt

# Run
hdfs dfs -rm -r /output
hadoop jar /src/invertedindex.jar InvertedIndex /input /output hdfs:///stopwords.txt
hdfs dfs -get /output/part-r-00000 /src/result.txt

# Exit and close cluster
exit
docker compose down
```

---

## Results

| Metric | Value |
|--------|-------|
| Input books | 3 (Moby Dick, Dracula, Pride and Prejudice) |
| Lines processed | 51,882 |
| Map tasks | 3 (parallel, one per book) |
| Unique words indexed | 24,722 |
| Output size | 5.68 MB |

Sample output (word → filename:lineNumber):
```
abbey       dracula.txt:3201, dracula.txt:3209, pride_prejudice.txt:89
abide       moby_dick.txt:4821, dracula.txt:6536, pride_prejudice.txt:712
captain     moby_dick.txt:234, moby_dick.txt:891, dracula.txt:445
```

Each entry shows the **exact line number** within the file where the word appears, making it easy to locate any word across all three books.

---

## Conclusion

The project demonstrates how Hadoop MapReduce can efficiently build an inverted index over large text datasets in a distributed way:

- **Parallel processing** — each book was processed simultaneously by a separate Map task
- **Stop word filtering** — common words were loaded once via Distributed Cache and filtered on every node
- **Line number tracking** — the Mapper maintains a line counter per file split, producing human-readable locations that meet the assignment requirement
- **Scalability** — adding more books or more DataNodes/NodeManagers requires no code changes

The Reducer merged all results into a single inverted index of **24,722 unique words** across three novels — the same core concept behind real-world search engines like Elasticsearch and Apache Solr.