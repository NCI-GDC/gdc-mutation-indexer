from parsers import (
    Parser,
    ESHadoopArgs,
    ESArgs,
    S3Args,
    BuildArgs,
    SparkArgs,
)


def test_build_args():
    parser = Parser.build([BuildArgs])
    import pdb; pdb.set_trace()
