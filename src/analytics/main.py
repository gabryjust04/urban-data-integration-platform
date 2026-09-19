from src.common.spark import create_spark_session
from src.analytics.queries import run_analytics
from src.analytics.data_products import build_data,use_new_data


def main():
    spark = create_spark_session()

    try:
        run_analytics(spark)
        build_data(spark)
        use_new_data(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()