# Running GDC Mutation Indexer for production

Export jobs may be submitted to a spark cluster using `bin/submit-job.sh`.
This will package an egg of the indexer source code and submit it via
`spark-submit`. Note that the `bin/submit-job.sh.temlpate` needs to be filled
in with the required credentials for various services or set inside `config.py`.
