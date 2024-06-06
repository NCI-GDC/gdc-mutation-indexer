from mutation_indexer.configuration import spark


class TestSpark:
    def test__get_arguments__flat(self) -> None:
        config = spark.Spark(
            master="name",
            app=None,
            driver=None,
            executor=None,
            pyspark=None,
            sql=None,
            submit=None,
            yarn=None,
        )

        args = frozenset(config.get_arguments())

        assert args == frozenset(
            (
                ("--conf", "spark.master=name"),
                ("--conf", "spark.app=None"),
                ("--conf", "spark.driver=None"),
                ("--conf", "spark.executor=None"),
                ("--conf", "spark.pyspark=None"),
                ("--conf", "spark.sql=None"),
                ("--conf", "spark.submit=None"),
                ("--conf", "spark.yarn=None"),
            )
        )

    def test__get_arguments__nested(self) -> None:
        config = spark.Spark(
            master="name",
            app=None,
            driver=None,
            executor=None,
            pyspark=spark.Pyspark(
                python="python3.X", driver=spark.PythonDriver(python="python3")
            ),
            sql=None,
            submit=None,
            yarn=None,
        )

        args = frozenset(config.get_arguments())

        assert args == frozenset(
            (
                ("--conf", "spark.master=name"),
                ("--conf", "spark.app=None"),
                ("--conf", "spark.driver=None"),
                ("--conf", "spark.executor=None"),
                ("--conf", "spark.pyspark.python=python3.X"),
                ("--conf", "spark.pyspark.driver.python=python3"),
                ("--conf", "spark.sql=None"),
                ("--conf", "spark.submit=None"),
                ("--conf", "spark.yarn=None"),
            )
        )

    def test__get_arguments__camel_case(self) -> None:
        config = spark.Spark(
            master="name",
            app=None,
            driver=spark.Driver(max_result_size="10g", memory="40g"),
            executor=None,
            pyspark=None,
            sql=None,
            submit=None,
            yarn=None,
        )

        args = frozenset(config.get_arguments())

        assert args == frozenset(
            (
                ("--conf", "spark.master=name"),
                ("--conf", "spark.app=None"),
                ("--conf", "spark.driver.maxResultSize=10g"),
                ("--conf", "spark.driver.memory=40g"),
                ("--conf", "spark.executor=None"),
                ("--conf", "spark.pyspark=None"),
                ("--conf", "spark.sql=None"),
                ("--conf", "spark.submit=None"),
                ("--conf", "spark.yarn=None"),
            )
        )

    def test__get_arguments__env(self) -> None:
        config = spark.Spark(
            master="name",
            app=None,
            driver=None,
            executor=None,
            pyspark=None,
            sql=None,
            submit=None,
            yarn=spark.Yarn(
                app_master_env=spark.Env(tmpdir="/tmp"),
                executor_env=spark.Env(tmpdir="/mnt/tmp"),
            ),
        )

        args = frozenset(config.get_arguments())

        print(args)

        assert args == frozenset(
            (
                ("--conf", "spark.driver=None"),
                ("--conf", "spark.yarn.executorEnv.TMPDIR=/mnt/tmp"),
                ("--conf", "spark.yarn.appMasterEnv.TMPDIR=/tmp"),
                ("--conf", "spark.submit=None"),
                ("--conf", "spark.executor=None"),
                ("--conf", "spark.sql=None"),
                ("--conf", "spark.pyspark=None"),
                ("--conf", "spark.app=None"),
                ("--conf", "spark.master=name"),
            )
        )
