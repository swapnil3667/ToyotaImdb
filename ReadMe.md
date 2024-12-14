## Toyota Imdb 

The objective is to utilize PySpark for evaluating the task at hand. 
We have been provided with the following datasets.

```
name.basics.tsv.gz
title.akas.tsv.gz
title.basics.tsv.gz
title.crew.tsv.gz
title.episode.tsv.gz
title.principals.tsv.gz
title.ratings.tsv.gz
```
I have made an effort to thoroughly address the following questions in my implementation.

### Problem 1:

Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
(numVotes/averageNumberOfVotes) * averageRating

#### Approach
1. Since we were interested in only movies with minimum of 500 votes , filtered datasets `title.ratings.tsv.gz` with votes > 500
2. Subsequently, I extracted only movies from the `title.basics.tsv.gz` dataset.
3. Performed an inner join on the title and rating datasets, subsequently sorting the resultant dataset in descending order based on the rating attribute.

### Problem 2:

For these 10 movies, list the persons who are most often credited and list the
different titles of the 10 movies.

##### Different titles of the movie
1. Utilized the top n movies identified in Problem 1
2. Executed an inner join of the aforementioned dataset with `title.akas.tsv.gz`.
3. Grouped them on tconst(title unique id) and primaryTitle and aggregated unique set of titles in a List 

##### List of person often credited
1. Utilized the top n movies identified in Problem 1.
2. Created a list important_person_category = ["actor", "actress", "director", "producer"]
3. Filtered `title.principals.tsv.gz` dataset for important person category 
4. Then joined it with top movies df on unique title id
5. finally did a groupby on title id , aggregate the list of unique person name associated with each title of top 10 movies 

###  Important files
The following are essential configuration parameters required for application execution:

 ***config file*** :  configs/config.json

 Please ensure that the input file paths are accurately updated prior to executing the job.
```$xslt
{
  "title_basic": "path/to/title.basics.tsv.gz",
  "title_rating": "path/to/title.ratings.tsv.gz",
  "title_akas": "path/to/title.akas.tsv.gz",
  "title_principal": "path/to/title.principals.tsv.gz",
  "name_basic":"path/to/name.basics.tsv.gz",
  "top_n": 10,
  "minimum_votes" : 500,
  "output_path": "output.csv"
}
``` 

***script*** : 

    1. run-local.sh: to run locally
    2. deploy-cluster.sh : template to deploy on spark cluster 

### Steps to run the code 

Install following in bash:
* make
* docker

Set of commands 

#### Build the package

You need to download sbt package manager. I have build a jar and kept
```$xslt
make clean
make build
```

#### Pull the Docker Image
```
docker pull apache/spark 
```

#### Docker RUN with shell script 
```
docker run -it -v $PWD:/opt/spark/work-dir apache/spark bash ./run-local.sh
```

### Note
Since I have been mostly till date Scala user for writing Spark application, to be open I took some motivation for pyspark directory structure from this [blog](https://pyspark-tdd-template.readthedocs.io/en/latest/intro.html)