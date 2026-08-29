from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DecimalType
)

SMS_RAW_SCHEMA = StructType(
    [
        StructField("ROAMSTATE_519", IntegerType(), True),
        StructField("CUST_LOCAL_START_DATE_15", LongType(), True),
        StructField("CDR_ID_1", LongType(), True),
        StructField("CDR_SUB_ID_2", IntegerType(), True),
        StructField("CDR_TYPE_3", StringType(), True),
        StructField("SPLIT_CDR_REASON_4", StringType(), True),
        StructField("RECORD_DATE", StringType(), True),
        StructField("PAYTYPE_515", IntegerType(), True),
        StructField("DEBIT_AMOUNT_42", DecimalType(18, 4), True),
        StructField("SERVICEFLOW_498", IntegerType(), True),
        StructField("EVENTSOURCE_CATE_17", StringType(), True),
        StructField("USAGE_SERVICE_TYPE_19", IntegerType(), True),
        StructField("SPECIALNUMBERINDICATOR_534", DecimalType(18, 4), True),
        StructField("BE_ID_30", DecimalType(18, 4), True),
        StructField("CALLEDPARTYIMSI_495", StringType(), True),
        StructField("CALLINGPARTYIMSI_494", StringType(), True),
    ]
)

REFERENCE_SCHEMA = StructType(
    [
        StructField("PayType", IntegerType(), False),
        StructField("value", StringType(), False),
    ]
)