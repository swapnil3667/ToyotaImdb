from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_set
from pyspark.sql.functions import lit
from jobs.pipeline import transform


def read_tsv(filepath: str, spark: SparkSession):
    df = (spark.read
          .option("header", "true")
          .option("sep", "\t")
          .csv(filepath))
    return df


if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()


    ratingDF = read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/title.ratings.tsv.gz", spark)
    titleBasicDF = read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/title.basics.tsv.gz", spark)



    moviesTitlesDF = titleBasicDF.filter(titleBasicDF["titleType"] == 'movie')

    titleRatingDF = read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/title.ratings.tsv.gz",spark)
    titleRatingDF = titleRatingDF.filter(titleRatingDF["numVotes"] > 500)
    print(titleRatingDF.count())

#     resultDF = (moviesTitlesDF
#      .join(titleRatingDF,moviesTitlesDF["tconst"] == titleRatingDF["tconst"], "inner").select(moviesTitlesDF["tconst"], moviesTitlesDF["primaryTitle"], moviesTitlesDF["originalTitle"], titleRatingDF["averageRating"]))
#     print(resultDF.schema)
#     resultDF.printSchema()
#
#     topMoviesDF = resultDF.orderBy(resultDF["averageRating"],ascending=False).limit(10)
#
#
#
#     """select titles of type movies and join with ratings select with minimum 500 votes"""
#
#     #get all titles
#     titlesAkasDF = read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/title.akas.tsv.gz",spark)
#     titlesAkasDF.show()
#     #join with titles names
#     moviesTitlesDFNew = (topMoviesDF.join(titlesAkasDF,topMoviesDF["tconst"] == titlesAkasDF["titleId"], "inner" )
#      .select(titlesAkasDF["titleId"],topMoviesDF["primaryTitle"],titlesAkasDF["title"]))
#     moviesTitlesDFNew.printSchema()
#    # moviesTitlesDFNew.show()
#     moviesTitlesDFNew.groupBy(moviesTitlesDFNew['titleId'],moviesTitlesDFNew['primaryTitle']).agg(collect_set(moviesTitlesDFNew['title']))
#
# # find out cast bname
#     titlePrinciplesDF =  read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/title.principals.tsv.gz",spark)
#
#     choice_list = ["actor", "actress","director", "producer"]
#     moviePrincipleDF = topMoviesDF.join(titlePrinciplesDF, topMoviesDF["tconst"] == titlePrinciplesDF["tconst"], "inner").select(topMoviesDF["tconst"],titlePrinciplesDF["nconst"],titlePrinciplesDF["category"],titlePrinciplesDF["job"],titlePrinciplesDF["characters"])
#     importantPersonDF = moviePrincipleDF.where(col("category").isin(choice_list)).select(col('tconst'),col('nconst'))
#
#     nameBasicDF = read_tsv("C:/Users/Siddhartha/PycharmProjects/ToyotaImdb/resources/name.basics.tsv.gz", spark)
#     nameBasicDF.show()
#     importantPersonDF.join(nameBasicDF, importantPersonDF["nconst"] == nameBasicDF["nconst"], "inner").groupBy(importantPersonDF['tconst']).agg(collect_set(col('primaryName'))).show()