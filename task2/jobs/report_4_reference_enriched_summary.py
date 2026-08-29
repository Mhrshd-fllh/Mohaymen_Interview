import os
import sys
from pathlib import Path
import argparse
import logging
import pandas as pd

TASK2_DIR = Path(__file__).resolve().parent.parent
if str(TASK2_DIR) not in sys.path:
    sys.path.insert(0, str(TASK2_DIR))

import src
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    broadcast,
    coalesce,
    lit,
    sum as spark_sum,
    count as spark_count,
    date_format,
)
from pyspark.sql.types import DecimalType

from src.spark_session import get_spark_session
from src.minio_utils import MinIOManager
from src.pipeline.schema import SMS_RAW_SCHEMA, REFERENCE_SCHEMA
from src.pipeline.transformers import prepare_sms_stream

logger = logging.getLogger("report_4_reference_enriched_summary")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def load_reference_table(spark: SparkSession, ref_file_path: str) -> DataFrame:
    logger.info("Loading reference table from: %s", ref_file_path)
    ref_path = Path(ref_file_path)
    if not ref_path.is_file():
        raise FileNotFoundError(f"Reference file not found at: {ref_file_path}")

    if ref_path.suffix.lower() in (".xlsx", ".xls"):
        pdf = pd.read_excel(ref_file_path)
    else:
        pdf = pd.read_csv(ref_file_path)

    pdf["PayType"] = pdf["PayType"].astype(int)
    pdf["value"] = pdf["value"].astype(str)

    ref_df = spark.createDataFrame(pdf, schema=REFERENCE_SCHEMA)
    logger.info("Successfully loaded reference table (%d rows).", ref_df.count())
    return ref_df

def build_enriched_summary_stream(
    raw_stream_df: DataFrame,
    reference_df: DataFrame,
    watermark_delay: str = "1 hour",
    window_duration: str = "15 minutes",
) -> DataFrame:

    logger.info("Applying standard SMS cleaning and currency transformations...")
    cleaned_df = prepare_sms_stream(raw_stream_df)

    logger.info("Registering event-time watermark: delayThreshold = %s on 'RECORD_DATE'", watermark_delay)
    watermarked_df = cleaned_df.withWatermark("RECORD_DATE", watermark_delay)

    logger.info("Executing Stream-Static Broadcast Join on paytype == PayType...")

    enriched_df = watermarked_df.join(
        broadcast(reference_df),
        watermarked_df.paytype == reference_df.PayType,
        how="left",
    ).withColumn("pay_type_label", coalesce(col("value"), lit("Unknown")))

    logger.info(
        "Configuring %s tumbling window aggregation grouped by (window, Pay type)...",
        window_duration,
    )
    windowed_aggregated_df = (
        enriched_df.groupBy(
            window(col("RECORD_DATE"), window_duration),
            col("pay_type_label").alias("Pay type"),
        )
        .agg(
            spark_count("*").alias("Record_Count"),
            spark_sum("revenue").cast(DecimalType(18, 2)).alias("revenue"),
        )
    )

    projected_df = windowed_aggregated_df.select(
        date_format(col("window.start"), "yyyy/MM/dd HH:mm:ss").alias("RECORD_DATE"),
        col("Pay type"),
        col("Record_Count"),
        col("revenue"),
    )

    return projected_df

def run_report_4(
    input_source_path: str,
    output_sink_path: str,
    checkpoint_path: str,
    reference_file_path: str,
    watermark_delay: str = "1 hour",
    window_duration: str = "15 minutes",
    sink_mode: str = "foreachBatch",
    trigger_interval: str = "10 seconds",
    once: bool = False,
):

    logger.info("Initializing MinIO infrastructure and health verification...")
    minio_mgr = MinIOManager()
    minio_mgr.ensure_pipeline_buckets()

    logger.info("Starting PySpark driver session...")
    spark = get_spark_session(app_name="Report_4_Reference_Enriched_Summary")

    reference_df = load_reference_table(spark, reference_file_path)

    logger.info("Reading input stream from: %s", input_source_path)
    raw_stream = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(SMS_RAW_SCHEMA)
        .load(input_source_path)
    )

    report_stream = build_enriched_summary_stream(
        raw_stream,
        reference_df,
        watermark_delay=watermark_delay,
        window_duration=window_duration,
    )

    logger.info("Configuring streaming sink (mode: '%s', once: %s)...", sink_mode, once)

    if sink_mode == "console":
        writer = (
            report_stream.writeStream.format("console")
            .outputMode("complete" if "window" not in report_stream.columns else "update")
            .option("truncate", "false")
        )
    elif sink_mode == "file":
        writer = (
            report_stream.writeStream.format("csv")
            .outputMode("append")
            .option("header", "true")
            .option("path", output_sink_path)
            .option("checkpointLocation", checkpoint_path)
        )
    elif sink_mode == "foreachBatch":
        def write_micro_batch(batch_df: DataFrame, batch_id: int):
            batch_count = batch_df.count()
            logger.info("Processing micro-batch %d: %d aggregated record(s) ready.", batch_id, batch_count)
            if batch_count > 0:
                batch_target_path = f"{output_sink_path.rstrip('/')}/batch_{batch_id}"
                sorted_batch = batch_df.orderBy("RECORD_DATE", "Pay type")
                sorted_batch.coalesce(1).write.mode("overwrite").option("header", "true").csv(batch_target_path)
                logger.info("Successfully committed micro-batch %d to: %s", batch_id, batch_target_path)

        writer = (
            report_stream.writeStream.outputMode("update")
            .foreachBatch(write_micro_batch)
            .option("checkpointLocation", checkpoint_path)
        )
    else:
        raise ValueError(f"Unsupported sink_mode: '{sink_mode}'. Choose 'file', 'foreachBatch', or 'console'.")

    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)

    query = writer.start()
    logger.info("Query successfully started. Query ID: %s | Run ID: %s", query.id, query.runId)
    logger.info("Streaming query is actively monitoring for incoming data...")
    query.awaitTermination()


if __name__ == "__main__":
    default_ref_path = str(TASK2_DIR.parent / "REF_SMS" / "Ref.xlsx")

    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming — Report 4: Reference Enriched Summary"
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default="s3a://telemetry-bucket/raw/",
        help="S3A or local path to incoming streaming CSV files.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="s3a://reports-bucket/reports/report_4_reference_enriched_summary/",
        help="S3A or local destination path for output CSV reports.",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default="s3a://reports-bucket/checkpoints/report_4_reference_enriched_summary/",
        help="S3A or local path for fault-tolerant state checkpoints.",
    )
    parser.add_argument(
        "--reference-file",
        type=str,
        default=default_ref_path,
        help="Path to static lookup reference file (Ref.xlsx).",
    )
    parser.add_argument(
        "--watermark-delay",
        type=str,
        default="1 hour",
        help="Allowed late event arrival threshold.",
    )
    parser.add_argument(
        "--window-duration",
        type=str,
        default="15 minutes",
        help="Tumbling window duration interval.",
    )
    parser.add_argument(
        "--sink-mode",
        type=str,
        choices=["file", "foreachBatch", "console"],
        default="foreachBatch",
        help="Streaming sink target implementation.",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default="10 seconds",
        help="Micro-batch trigger execution interval.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all currently available streaming data in a single batch and exit.",
    )

    args = parser.parse_args()
    run_report_4(
        input_source_path=args.input_path,
        output_sink_path=args.output_path,
        checkpoint_path=args.checkpoint_path,
        reference_file_path=args.reference_file,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        sink_mode=args.sink_mode,
        trigger_interval=args.trigger_interval,
        once=args.once,
    )