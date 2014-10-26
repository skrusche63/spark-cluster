![Elasticworks.](https://raw.githubusercontent.com/skrusche63/spark-cluster/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of predictive analytics to Elasticsearch.

## Reactive Similarity Analysis Engine

![Similarity Analysis Engine Overview](https://raw.githubusercontent.com/skrusche63/spark-cluster/master/images/similarity-analysis-overview.png)

The Similarity Analysis Engine is one of the nine members of the open ensemble and is built to find relevant similarities in dynamic activity sequences and 
identify customers by their journeys. It is based on the new S2MP algorithm to find similarities for sequential patterns in large scale datasets. 


#### Similarity of Sequential Patterns (S2MP)

Computing the similarity of sequential patterns is an important task to find regularities in sequences of data. This is the key to understand customer behavior, build profiles and signatures, and also to group similar customers by their temporally behavior.    

The similarity of sequential patterns may be used to cluster, (a) the content of sequence databases to retrieve more homogeneous datasets for sequential pattern mining, or (b) to group the respective mining results. The latter approach is used for customer segmentation based on similar engagement behavior.

For real-world applications, it is important to measure the similarity of more complex sequences, built from itemsets rather than single items. A customer's purchase behavior is an example of such a more complex sequence.

There exist already some similarity measures such as `Edit distance` (Levenshtein, 1996) and `LCS` (Longest Common Subsequence, 2002), but these methods do not take the content of the itemsets and their order and position in the sequences properly into account.

We therefore decided to implement the `S2MP` similarity measure proposed by [Saneifar et al](http://crpit.com/confpapers/CRPITV87Saneifar.pdf), which successfully overcomes the mentioned shortcomings.

From the similarity measure `sim(i,j)` of two sequences `i`and `j` it is straightforward to build the sequence engagement vector for sequence `i` with all other sequences. These vectors may then be used to build clusters with algorithms such as KMeans.

In market basket analysis or web usage mining, a sequence of purchase transactions or web sessions is directly associated with a certain customer or visitor. The clusters built from KMeans and S2MP may then be applied to group customers with similar buying or web usage behavior.

---

#### Clustering of Sequential Patterns (S-KMeans)

Clustering of sequential data or patterns becomes more and more relevant for business applications. `SKMeans` is a modified version of Apache Spark's KMeans algorithm and is optimized for clustering sequential patterns based on the `S2MP` similarity measure. 


