from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, round as spark_round
from pyspark.sql.types import DecimalType

def parse_event_timestamps(
    df: DataFrame,
    source_col: str = "RECORD_DATE",
    target_col: str = "RECORD_DATE",
    date_format: str = "yyyy/MM/dd HH:mm:ss",
) -> DataFrame:

    return df.withColumn(target_col, to_timestamp(col(source_col), date_format))


def convert_milli_rial_to_toman(
    df: DataFrame,
    source_col: str = "DEBIT_AMOUNT_42",
    target_col: str = "revenue",
) -> DataFrame:

    clean_debit = coalesce(col(source_col), lit(0))
    toman_expr = (clean_debit / lit(10000)).cast(DecimalType(18, 4))
    return df.withColumn(target_col, toman_expr)


def prepare_sms_stream(raw_df: DataFrame) -> DataFrame:
    parsed_df = parse_event_timestamps(
        raw_df,
        source_col="RECORD_DATE",
        target_col="RECORD_DATE",
        date_format="yyyy/MM/dd HH:mm:ss"
    )

    toman_df = convert_milli_rial_to_toman(
        parsed_df,
        source_col="DEBIT_AMOUNT_42",
        target_col="revenue"
    )

    standardized_df = toman_df.withColumn("paytype", col("PAYTYPE_515"))

    return standardized_df