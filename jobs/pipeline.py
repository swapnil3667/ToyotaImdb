from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_set
from typing import Dict


def extract_tsv(filepath: str, spark: SparkSession):
    df = (spark.read
          .option("header", "true")
          .option("sep", "\t")
          .csv(filepath))
    return df


def transform_top_movies(title_basic_df: DataFrame, title_rating_df: DataFrame, config: Dict) -> DataFrame:
    movies_titles_df = title_basic_df.filter(title_basic_df["titleType"] == 'movie')

    title_rating_df = title_rating_df.filter(col("numVotes") >= config["minimum_votes"])

    transformed_df = (movies_titles_df
                .join(title_rating_df, movies_titles_df["tconst"] == title_rating_df["tconst"], "inner").select(
        movies_titles_df["tconst"], movies_titles_df["primaryTitle"], movies_titles_df["originalTitle"],
        title_rating_df["averageRating"]))

    result_df = transformed_df.orderBy(transformed_df["averageRating"], ascending=False).limit(config["top_n"])
    return result_df


def transform_movie_titles(title_akas_df: DataFrame, top_movies_df: DataFrame) -> DataFrame:
    result_df = (top_movies_df.join(title_akas_df, top_movies_df["tconst"] == title_akas_df["titleId"], "inner")
                 .groupBy(title_akas_df['titleId'], top_movies_df['primaryTitle']).agg(
        collect_set(title_akas_df['title']).alias("titleList")))
    return result_df


def transform_movie_person(top_movies_df: DataFrame, title_principal_df: DataFrame,
                           name_basic_df: DataFrame) -> DataFrame:
    important_person_category = ["actor", "actress", "director", "producer"]

    title_principal_df = title_principal_df.where(col("category").isin(important_person_category))

    important_person_df = top_movies_df.join(title_principal_df, top_movies_df["tconst"] == title_principal_df["tconst"],
                                            "inner").select(top_movies_df["tconst"], title_principal_df["nconst"])

    result_df = important_person_df.join(name_basic_df, important_person_df["nconst"] == name_basic_df["nconst"],
                                         "inner").groupBy(important_person_df['tconst']).agg(collect_set(col('primaryName')).alias("personList"))
    return result_df


def load(df: DataFrame, config: Dict, logger) -> bool:
    df.show()
    # df.limit(10).write.save(path=config["output_path"],mode="overwrite")
    return True


def run(spark: SparkSession, config: Dict, logger) -> bool:
    logger.warn("starting pipeline")
    title_basic_df = extract_tsv(config["title_basic"], spark)
    title_rating_df = extract_tsv(config["title_rating"], spark)
    top_movies_df = transform_top_movies(title_basic_df, title_rating_df, config)
    top_movies_df.cache()
    load(top_movies_df, config, logger)

    title_akas_df = extract_tsv(config["title_akas"], spark)
    top_movie_titles_df = transform_movie_titles(title_akas_df, top_movies_df)
    load(top_movie_titles_df, config, logger)

    title_principal_df = extract_tsv(config["title_principal"], spark)
    name_basic_df = extract_tsv(config["name_basic"], spark)
    top_movie_person_df = transform_movie_person(top_movies_df, title_principal_df, name_basic_df)
    load(top_movie_person_df, config, logger)

    logger.warn("pipeline is complete")
    spark.stop()
    return True
