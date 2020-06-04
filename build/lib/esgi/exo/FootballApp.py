from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import datetime

def main():
    spark = createSparkSession()
    matchesDF = dataFrameFromCsv(spark)
    matchesDF = clearDataFrame(matchesDF)
    matchesDF.orderBy([matchesDF.penalty_france], ascending=[0,1]).show()

def createSparkSession():
    return (SparkSession.builder
            .appName('FootballApp')
            .config('spark.ui.port','5050')
            .getOrCreate())

def dataFrameFromCsv(spark):
    return spark.read.csv('data/df_matches.csv', header=True, sep=",")

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