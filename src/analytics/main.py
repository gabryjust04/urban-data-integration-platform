from src.common.spark import create_spark_session
from src.analytics.queries import run_analytics


def main():
    spark = create_spark_session()

    try:
        run_analytics(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()