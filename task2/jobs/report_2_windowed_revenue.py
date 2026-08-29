import os
import sys
from pathlib import Path
import argparse
import logging

TASK2_DIR = Path(__file__).resolve().parent.parent
if str(TASK2_DIR) not in sys.path:
    sys.path.insert(0, str(TASK2_DIR))

import src
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, window, sum as spark_sum, count as spark_count, date_format
from pyspark.sql.types import DecimalType

from src.spark_session import get_spark_session
from src.minio_utils import MinIOManager
from src.pipeline.schema import SMS_RAW_SCHEMA
from src.pipeline.transformers import prepare_sms_stream

logger = logging.getLogger("report_2_windowed_revenue")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def build_windowed_revenue_stream(
    raw_stream_df: DataFrame,
    watermark_delay: str = "1 hour",
    window_duration: str = "15 minutes",
) -> DataFrame:

    logger.info("Applying standard SMS cleaning and currency transformations...")
    cleaned_df = prepare_sms_stream(raw_stream_df)

    logger.info("Registering event-time watermark: delayThreshold = %s on 'RECORD_DATE'", watermark_delay)
    watermarked_df = cleaned_df.withWatermark("RECORD_DATE", watermark_delay)

    logger.info("Configuring %s tumbling window aggregation grouped by (window, paytype)...", window_duration)
    windowed_aggregated_df = (
        watermarked_df.groupBy(
            window(col('RECORD_DATE'), window_duration),
            col("paytype")
        )
        .agg(
            spark_sum("revenue").cast(DecimalType(18, 2)).alias("revenue"),
            spark_count("*").alias("Record_Count")
        )
    )

    projected_df = windowed_aggregated_df.select(
        date_format(col("window.start"), "yyyy/MM/dd HH:mm:ss").alias("RECORD_DATE"),
        col("paytype").alias("Pay type"),
        col("revenue"),
        col("Record_Count")
    )
    return projected_df



def run_report_2(
    input_source_path: str,
    output_sink_path: str,
    checkpoint_path: str,
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
    spark = get_spark_session(app_name="Report_2_Windowed_Revenue_per_paytype")

    logger.info("Reading input stream from: %s", input_source_path)
    raw_stream = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(SMS_RAW_SCHEMA)
        .load(input_source_path)
    )

    report_stream = build_windowed_revenue_stream(
        raw_stream,
        watermark_delay=watermark_delay,
        window_duration=window_duration
    )

    logger.info("Configuring streaming sink (mode: %s, once: %s)...", sink_mode, once)

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
                # Order by RECORD_DATE and Pay type for clean readability
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
    parser = argparse.ArgumentParser(description="Spark Structured Streaming — Report 2: 15-Minute Revenue per PayType")
    parser.add_argument(
        "--input-path",
        type=str,
        default="s3a://telemetry-bucket/raw/",
        help="S3A or local path to incoming streaming CSV files.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="s3a://reports-bucket/reports/report_2_windowed_revenue/",
        help="S3A or local destination path for output CSV reports.",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default="s3a://reports-bucket/checkpoints/report_2_windowed_revenue/",
        help="S3A or local path for fault-tolerant state checkpoints.",
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
    run_report_2(
        input_source_path=args.input_path,
        output_sink_path=args.output_path,
        checkpoint_path=args.checkpoint_path,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        sink_mode=args.sink_mode,
        trigger_interval=args.trigger_interval,
        once=args.once,
    )