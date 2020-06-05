from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.window import Window

import datetime

def main(): 
    sparkSession = createSparkSession()
    matchesDF = dataFrameFromCsv(sparkSession)
    matchesDF = clearDataFrame(matchesDF)
    matchesDF = addHomeGameColumn(matchesDF)
    createStatsFile(matchesDF)
    createJoinFile(sparkSession, matchesDF)

def createSparkSession():
    return (SparkSession.builder
            .appName('FootballApp')
            .config('spark.ui.port','5050')
            .getOrCreate())

def dataFrameFromCsv(sparkSession):
    return sparkSession.read.csv('data/df_matches.csv', header=True, sep=",")

def clearDataFrame(matchesDF):
    matchesDF = renameColMatchAndCompet(matchesDF)
    matchesDF = selectSpecificsColumns(matchesDF)
    matchesDF = transformNullValuesPenalty(matchesDF)
    matchesDF = filterOnMatchesDates(matchesDF)
    return matchesDF
    

def renameColMatchAndCompet(matchesDF):
    matchesDF = matchesDF.withColumnRenamed('X4', 'match')
    matchesDF = matchesDF.withColumnRenamed('X6', 'competition')
    return matchesDF

def transformNAToZero(penalty):
    if(penalty == "NA"):
        return "0"
    return penalty

def transformNullValuesPenalty(matchesDF):
    convert_na_to_zero_udf = F.udf(transformNAToZero, StringType())
    matchesChangePenaltyFranceDF = matchesDF.withColumn('penalty_france', convert_na_to_zero_udf(matchesDF.penalty_france))
    matchesChangePenaltyFranceAdversaireDF = matchesChangePenaltyFranceDF.withColumn('penalty_adversaire', convert_na_to_zero_udf(matchesChangePenaltyFranceDF.penalty_adversaire))
    return matchesChangePenaltyFranceAdversaireDF

def selectSpecificsColumns(matchesDF):
    return matchesDF.select('match',
                            'competition',
                            'adversaire',
                            'score_france',
                            'score_adversaire',
                            'penalty_france',
                            'penalty_adversaire',
                            'date')

def filterOnMatchesDates(matchesDF):
    convert_error_to_zero_udf = F.udf(convertErrorToZero, StringType())
    matchesDF = matchesDF.withColumn('date', convert_error_to_zero_udf(matchesDF.date))
    return matchesDF.filter((matchesDF.date != "0") & (matchesDF.date >= '1980-03-01'))

def convertErrorToZero(date):
    dateString = str(date)
    try:
        datetime.datetime.strptime(dateString, '%Y-%m-%d')
        return date
    except ValueError:
        return "0"

def addHomeGameColumn(matchesDF):
    define_domicile_udf = F.udf(lambda match: True if (match[:8] == "France -") else False, BooleanType())
    matchesDF = matchesDF.withColumn('domicile', define_domicile_udf(matchesDF.match))
    return matchesDF

def createStatsFile(matchesDF):
    statisticsDF = calculateStatistics(matchesDF)
    statisticsDF.write.mode("overwrite").parquet("data/stats.parquet/")

def calculateStatistics(matchesDF):
    cdm_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

    agg_stats = (matchesDF
        .groupBy("adversaire")
        .agg(

            F.avg(matchesDF.score_france).alias("moy_score_france"),
            F.avg(matchesDF.score_adversaire).alias("moy_score_adversaire"),
            F.count(matchesDF.adversaire).alias("nb_match"),
            (F.sum(matchesDF.domicile.cast("int")) * 100 /  F.count(matchesDF.adversaire)).alias("pourcent_match_domicile"),
            cdm_cond(F.col("competition").contains("Coupe du monde")).alias("nb_match_cdm"),
            F.max(matchesDF.penalty_france.cast("int")).alias("max_france_penalty"),
            ( F.sum(matchesDF.penalty_france.cast("int")) - F.sum(matchesDF.penalty_adversaire.cast("int")) ).alias("nb_pen_fr_moins_pen_advrs")
        )
    )

    return agg_stats

def createJoinFile(sparkSession, matchesDF):
    statisticsDF = sparkSession.read.parquet("data/stats.parquet/")
    matchesDF = matchesDF.join(statisticsDF, "adversaire")

    matchesDF = matchesDF.withColumn("annee", F.year(matchesDF.date))
    matchesDF = matchesDF.withColumn("mois", F.month(matchesDF.date))

    matchesDF.write.partitionBy("annee").mode('overwrite').parquet('data/result.parquet/')
    matchesDF.write.partitionBy("mois").mode('append').parquet('data/result.parquet/')
