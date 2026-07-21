"""
smoke_test.py — Phase 5.2: kiểm tra chuỗi kết nối Spark -> MinIO (S3A) -> Parquet.

Mục đích: xác nhận Spark ĐỌC được dữ liệu landing trên MinIO TRƯỚC khi viết job ingest thật.
Đọc bảng `questions` từ s3a://landing/, in schema + vài dòng + đếm + phân bố theo part.

Submit: bash processing/spark/submit.sh smoke_test.py
"""

from pyspark.sql import SparkSession


def main() -> None:
    """Đọc thử bảng questions từ MinIO và in thông tin kiểm chứng."""
    spark = SparkSession.builder.appName("smoke_test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("s3a://landing/questions/")

    print(">>> Schema của questions:")
    df.printSchema()

    print(">>> 5 dòng đầu:")
    df.show(5, truncate=False)

    print(">>> Tổng số câu hỏi:", df.count())

    print(">>> Phân bố theo part (kiểm chứng skew Part 5):")
    df.groupBy("part").count().orderBy("part").show()

    spark.stop()
    print(">>> SMOKE TEST OK: Spark đọc được Parquet từ MinIO qua S3A.")


if __name__ == "__main__":
    main()
