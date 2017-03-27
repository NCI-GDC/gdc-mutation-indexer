import os
import uuid
from elasticsearch import Elasticsearch


class BaseConfig(object):
    # The Spark application name
    app_name = 'GDC_Mutation_Export'
    #spark_master = 'spark://dev-master-av2-dev2-dkolbman-notebook-0:7077'
    spark_master = 'local[1]'

    api_host = os.getenv('API_HOST', 'http://api.service.consul')
    signpost_host = os.getenv('SIGNPOST_HOST', 'http://signpost.service.consul')
    s3_host = os.getenv('S3_HOST', 'http://cleversafe.service.consul')
    s3_bucket = 's3a://gdc-mutation-indexer/'
    # This is the cluster where document will be loaded into
    es_host = os.getenv('ES_HOST', 'http://localhost')
    es_port = os.getenv('ES_PORT', 9200)
    es_user = os.getenv('ES_USER', '')
    es_pass = os.getenv('ES_PASS', '')

    # Debug mode
    debug = False

    # Index names, these also double as document type names
    # If name is None, the index will not be built
    index_names = {
        'case_centric':           'case_centric',
        'gene_centric':           'gene_centric',
        'ssm_centric':            'ssm_centric',
        'ssm_occurrence_centric': 'ssm_occurrence_centric'
    }
    # Where to save each index's final json
    index_paths = {
        'case_centric':           s3_bucket+'case-centric.json',
        'gene_centric':           s3_bucket+'gene-centric.json',
        'ssm_centric':            s3_bucket+'ssm-centric.json',
        'ssm_occurrence_centric': s3_bucket+'ssm-occurrence-centric.json'
    }
    # Whether to save the indices once they've been built
    index_keep = False
    # Load a prebuilt index and load it into elasticsearch
    index_use_existing = False
    # Whether to overwrite a built index file, if it exists
    index_overwrite = True
    # How many partitions to distribute the index file accross
    # The index will be split up into this many json files
    index_partitions = 1024

    mappings = {
                'ssm': 'ssm.yml',
                'gene': 'gene.yml',
                'transcript': 'transcript.yml',
                'annotation': 'annotation.yml',
                'observation': 'observation.yml',
                }

    # Index revision number, will be determined automatically if not specified
    revision = None

    # Used for loading case/graph documents from a different es cluster
    source_es_host = os.getenv('SOURCE_ES_HOST',
                               'http://localhost')
    source_es_port = os.getenv('SOURCE_ES_PORT', 9200)
    graph_index = os.getenv('SOURCE_ES_INDEX', 'gdc_from_graph')
    graph_document = os.getenv('SOURCE_ES_DOCUMENT', 'case')

    # Namespace for ssm_ids so that they may be reproduced
    ssm_namespace = uuid.UUID('d15296a3-38ed-412e-8ace-75e235f82f55')

    # The location of the gene model json
    gene_model_file = 's3a://test/genes.hg38.v2.json'
    citobands_file = 's3a://test/genes.cytobands.tsv.gz'
    census_file = 's3a://test/cancer_gene_census_set.tsv.gz'

    # Locations of MAFs to combine. If none, all public paths listed on the
    # the portal will be combined and used

    muse_urls = [
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ACC.muse.be42922b-5b8c-48c0-b9b7-7aad2bfb87fc.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BLCA.muse.cf549ca2-4aa2-4c7c-90d0-e917e700aa98.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BRCA.muse.6fcfff20-4993-4789-b27b-69d165130466.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CESC.muse.63d99c6c-c71b-4240-8b20-13174f593cd1.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CHOL.muse.cef59520-3564-450e-8eff-cb2cab35bb20.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.COAD.muse.e3c44af1-9e65-45df-b780-005b528cf294.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.DLBC.muse.574d161b-aec7-4841-95b1-ae003fcef466.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ESCA.muse.d150d4c0-10be-483f-8cbc-a198a31e5316.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.GBM.muse.7e85de23-3855-4279-a3ac-a81827e4ccb6.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.HNSC.muse.7f30fea6-bc2d-47b2-92bd-03542d2238f7.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KICH.muse.ab053ef1-1a06-4eb0-b29d-180651d70e2c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRC.muse.9902a865-7477-4a5e-ac7f-1e42e693812d.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRP.muse.8b6cab7e-afb5-4cf2-afb3-9dbd8f94e3ce.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LAML.muse.ddd7ac1c-0d23-4e36-b15f-0ecdb46663c5.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LGG.muse.8c521454-cbac-4ea8-8814-8dbd199df94e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LIHC.muse.2a2d3f3c-6b69-42c2-8057-3ba7d69fb833.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUAD.muse.6fb7067d-10f3-4615-af27-036d52160714.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUSC.muse.d761366c-9e61-42b3-8592-8bf61632b0e2.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.MESO.muse.411c650f-c938-4bd7-b958-3543ba6eaa47.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.OV.muse.6410f811-2801-465a-8748-0dcd0b52fea0.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PAAD.muse.8fb5a06c-a022-40d3-afec-7c130c866e05.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PCPG.muse.72ac40d6-31b7-4598-8fe4-06d4fb886b05.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PRAD.muse.6ec1488d-1fa5-44df-ba41-aaad4db0f84d.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.READ.muse.c6c7cc68-dc7d-40aa-a15f-1833d7ccad1a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SARC.muse.a6865577-d27c-4789-abe1-631b9f152a9d.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SKCM.muse.bc35087a-858d-4859-8183-ddf3e342acac.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.STAD.muse.0e82dee2-b383-4ac5-8142-8ff45e76fd90.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.TGCT.muse.a2fb07f1-ba54-43dd-92cb-72cbe7f4a896.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THCA.muse.7b4b4699-16eb-4a7a-92ef-38ebce2fefa7.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THYM.muse.0e75d253-14bf-4f48-9cbf-4ddb7b0dede1.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCEC.muse.1093fec9-6c39-4589-adeb-95f00e5e2181.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCS.muse.ac2e6571-08f8-4bcb-b821-4cc4c0e2eaeb.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UVM.muse.e7825768-fe7b-4525-8334-5a675a57a75d.somatic.maf.gz']

    somaticsniper_urls = [
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRP.somaticsniper.02f0d869-188d-45a9-b8ff-156b39d86bfe.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KICH.somaticsniper.b1d7a5c5-3292-4be3-88a5-0e9aaa076409.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LGG.somaticsniper.ab9ddffc-1f1f-43c9-83a6-f8e1ef7fb2d5.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.GBM.somaticsniper.63cb25e6-7954-4c73-989b-5f87ef1e5d0b.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ESCA.somaticsniper.98b018f5-34b4-4672-9355-733f881a5bd3.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRC.somaticsniper.35c7d513-6562-4d07-8a77-daa9f3b0c200.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.DLBC.somaticsniper.011e9369-a30c-49d9-aa36-86c32ec8a7f8.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LAML.somaticsniper.2da7fc47-4910-48f8-b12d-e2085a74db85.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ACC.somaticsniper.5c113608-3315-43be-9a23-53ba80b6eb55.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BLCA.somaticsniper.100a88f4-7b9f-464c-966e-4f339c6a3393.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BRCA.somaticsniper.8855730e-21ad-4711-9be2-b58bc337834e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CESC.somaticsniper.82fcd657-bad4-4e94-946b-2f160688421e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CHOL.somaticsniper.b93b1661-7cbc-48f9-830c-f3b9fb099bde.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.COAD.somaticsniper.e65f106a-ac7f-4ead-b2bc-e53be4319650.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.HNSC.somaticsniper.fe0d8db3-69d0-4509-9290-23f1d9e10378.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LIHC.somaticsniper.a3bd1898-227e-45b0-bd0b-10f9494ea973.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUAD.somaticsniper.6b4d6aaf-17c1-4a66-a124-22c7d96f93f8.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUSC.somaticsniper.283d40b5-a656-4179-9a8f-a2f6e06fb900.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.MESO.somaticsniper.2b27eb4f-b020-4643-9ddf-6157a08acf57.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.OV.somaticsniper.c2def862-0136-410d-844c-7491ec01acbf.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PAAD.somaticsniper.1c5de7c5-9d5b-48df-a2e9-920c2849e9b0.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PCPG.somaticsniper.d83b148c-fce2-4a95-80f2-b9cd1f3f544d.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PRAD.somaticsniper.95d4e7a9-91a8-4e28-9dae-1c769c891788.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.READ.somaticsniper.a1837f69-dc6c-4a5f-9096-c30ab7a578b3.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SARC.somaticsniper.55e5bdff-e110-402b-a5ca-6251d8bf4daf.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SKCM.somaticsniper.ff7b5421-ae80-4c41-9b06-fcdb60aa1c6e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.STAD.somaticsniper.7a602b4d-c486-40c1-8b2c-e843c4692de1.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.TGCT.somaticsniper.966ebf28-43c2-4775-b4a3-c1b69a752f63.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THCA.somaticsniper.4cf03990-d1a7-408a-b40b-5cef535f3d6a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THYM.somaticsniper.17ed4740-1272-4784-85c5-115c48a35d71.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCEC.somaticsniper.feed966a-fb58-43b6-8fea-54b09e743133.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCS.somaticsniper.9d8bd20b-7d42-41fa-87ca-52ad3d1313b3.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UVM.somaticsniper.a6d600dc-0c81-48ce-a97b-de40846f36ef.somatic.maf.gz']

    varscan_urls = [
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ACC.varscan.1d6a8317-f66f-4b65-a93b-3990274bf3d8.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BLCA.varscan.7f6d7c84-07b9-438f-9516-5e70c980599f.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BRCA.varscan.053e3955-5fbd-46d3-8a21-c3cde48ef9b4.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CESC.varscan.11486c18-dd0d-41e0-8e5a-8256fe06308a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CHOL.varscan.f3c21968-53b6-4a60-8e51-afefccd07499.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.COAD.varscan.a0dd3426-6676-4451-b5ee-9e4dcbf9ef6f.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.DLBC.varscan.39bd9505-80cd-43d8-a1b6-d924bd586716.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ESCA.varscan.72ea4e14-9d56-4f02-8fdf-9e8f10fea054.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.GBM.varscan.cee264aa-62c7-4db1-a3fc-8e5df0086ef3.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.HNSC.varscan.7a1fcc7c-5e93-440d-a020-fcf202c61b48.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KICH.varscan.aa699d4e-4d46-41a5-8651-e83ff1d618da.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRC.varscan.b534e377-d359-4089-a68b-9988e430556c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRP.varscan.910e05df-8e69-4812-bb44-d337ffe4437a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LAML.varscan.273c2a4d-eefc-4adb-a8b3-a02662b06c84.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LGG.varscan.0b4f69bd-e32e-484f-9948-6934ea230c4a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LIHC.varscan.7fae10c8-aa3d-4d12-afd0-94e4bb6c1e66.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUAD.varscan.75cc3e01-ebed-44b8-8920-d07f0fcf39b1.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUSC.varscan.67c275f1-6c67-4a49-8c07-676121c7dd7e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.MESO.varscan.ee848264-5230-4cbc-a9bc-65b2b698d61a.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.OV.varscan.27e1361f-60cc-494b-9825-39f94f7ccec2.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PAAD.varscan.6ffc2fb8-4002-41fd-b4d1-da1645874d30.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PCPG.varscan.7c80ec05-da89-4900-9871-03ed695cb959.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PRAD.varscan.a2924fe5-517a-4b69-9669-1ae1b72ad083.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.READ.varscan.af7880ce-86da-4f1d-851b-f28d7ed481e3.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SARC.varscan.3397f4b6-6092-4934-9a24-fb44dd917d62.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SKCM.varscan.72c550ce-0933-4e0c-b38a-ccdd726c66e1.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.STAD.varscan.243830c4-1acc-49ec-a116-6f32914ccadb.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.TGCT.varscan.59ef1e7e-7c6d-495c-8014-2b93d393b99d.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THCA.varscan.ee220317-c8ca-4a7b-8ccc-b7141ed42efc.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THYM.varscan.59256aea-371a-46fb-999c-55e3ba6c8645.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCEC.varscan.8d418c30-6458-4b79-8630-eb037d2d8475.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCS.varscan.d99e0b0b-469a-46a5-a798-22034bfd380b.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UVM.varscan.f7cd83ef-9549-4448-a107-894e41e603b9.somatic.maf.gz']

    mutect_urls = [
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ACC.mutect.57478d37-f425-4889-b4ea-e82309a4244e.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BLCA.mutect.412c1711-564b-4c25-bd5f-320696a4dfb6.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.BRCA.mutect.96983226-d92a-449d-8890-e1b210cee0fe.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CESC.mutect.90399958-d30d-4b07-9fca-e7f9fd34534c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.CHOL.mutect.0cb835cf-7795-4422-9bdb-82778c640253.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.COAD.mutect.af65d530-7976-4cd0-8ec5-2af0f4dbb3a6.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.DLBC.mutect.aa0fe69e-aac2-4cea-935d-c7ecd9751372.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.ESCA.mutect.1b463a5c-153c-499e-ac1f-f0c909d6ff8f.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.GBM.mutect.195dab4b-31ae-4e1d-9e76-ff95bd1f9a23.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.HNSC.mutect.6c4df857-be5f-4850-b28e-b801fb5bdd7c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KICH.mutect.72f7a89c-a0d2-4f3f-9f4c-816e9febd284.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRC.mutect.f3992fbc-45e4-4f33-97c8-ab3636235cfc.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.KIRP.mutect.b286dd57-b803-4081-90ed-90549f2cd7a7.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LAML.mutect.2acf9a85-3ed1-4155-9485-a443a8c1b9b2.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LGG.mutect.2969b2f9-290b-417a-96d2-811713970ecb.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LIHC.mutect.c5c3865b-2803-4ebb-8693-30f2311479ea.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUAD.mutect.fc06e5d5-dd99-490f-9a4b-b3029df1e974.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.LUSC.mutect.b69bc4f4-e9c0-47f9-a874-383256b76bc7.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.MESO.mutect.68e05b3d-bc7a-4c34-bf30-09ee4733a941.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.OV.mutect.46d8e0d1-9d69-4a6a-98d5-69958b9d179b.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PAAD.mutect.78c75c50-e79e-4585-afb9-c674b5026708.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PCPG.mutect.71ed254c-6ce3-4844-a326-c897130b00c8.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.PRAD.mutect.5dd3362e-cf0f-47ab-b809-56b41d0cc57c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.READ.mutect.ffb80770-fb6a-436a-af3d-71665580b391.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SARC.mutect.53e07c68-8a5e-473e-ba77-bc4b1c075548.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.SKCM.mutect.c73bb755-b2ff-473c-a6ed-b771f5de531c.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.STAD.mutect.a88b4065-34b4-4858-9c16-55def79c38f2.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.TGCT.mutect.32e3b5b1-694e-44ad-a799-84fa0e3fb487.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THCA.mutect.9ced960d-0797-4a56-a45e-bc54ec25d022.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.THYM.mutect.403b4fed-558c-46e0-b7f4-99794e984164.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCEC.mutect.934cc4d2-56f3-40e1-a359-75f4e60294d5.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UCS.mutect.80b63a24-1df5-49e6-887a-4b2f7b4d6405.somatic.maf.gz',
            's3a://gdc-mutation-indexer/mafs-case-id-20170303/TCGA.UVM.mutect.a4df17ae-b540-4ddb-9cc4-32c369b57d57.somatic.maf.gz']

    #somaticsniper: 2227614  2.6GB
    #muse: 2730127  3.1GB
    #varscan: 2782495  3.2GB
    #mutect: 3416739  3.9GB

    maf_urls = mutect_urls[0:8]
    #maf_urls = varscan_urls + muse_urls

    # The location of the combined maf file
    maf_path = 's3a://test/uat_mafs.csv'
    # Whether to save the maf file or discard it when done
    maf_keep = False
    # Use combined maf if it already exists
    maf_use_existing = False
    # Whether to overwrite the combined maf file if it exists
    maf_overwrite = True

    percentile_threshold = {
        'genes_per_case': 100,
        'occurrences_per_ssm': 100,
        'consequences_per_ssm': 100,
        'observations_per_ssm': 100,
    }

    coalesce = 10
    repartition = 2048
    batch_size_bytes = '5mb'
    batch_size_entries = '100'

    # Case load settings
    case_exclude_fields = ','.join(['samples',
                                    'annotations',
                                    'days_to_index',
                                    'summary.file_size',
                                    'summary.file_count',
                                    'summary.experimental_strategies',
                                    'diagnoses.treatments',
                                    'tissue_source_site',
                                    'exposures',
                                    'family_histories',
                                    'files',
                                    '*_ids'])

    citobands_file = 's3a://test/genes.cytobands.tsv.gz'
    census_file = 's3a://test/cancer_gene_census_set.tsv.gz'
    gene_model_file = 's3a://test/genes.json'

    def __init__(self):
        self.indices = self.get_index_prefixes()

    def get_index_prefixes(self):
        '''
        Uses the version specified in the config, or will resolve the next
        version number by looking for an existing index and incrementing by one

        Eg:
            No indices exist in ES:
                index_name='case_centric' -> gdc_r0_case_centric

            gdc_r1_case_centric and gdc_r6_case_centric exist in ES:
                index_name='case_centric' -> gdc_r7_case_centric
        '''
        es = Elasticsearch(self.es_host,
                           port=self.es_port,
                           http_auth=(self.es_user, self.es_pass))

        def get_indices_max_version():
            versions = []
            indices = es.indices.get_alias().keys()

            for index_name in self.index_names.values():
                if index_name is not None:
                    versions = (versions + [int(v.split('_')[1].replace('r',''))
                                for v in indices if v.endswith(index_name)
                                            and v[:4] == 'gdc_'])

            if versions == []:
                version = 0
            else:
                version = max(versions) + 1
            return version

        def get_prefix(index_name):
            version = get_indices_max_version()
            prefix = 'gdc_r{}_{}'.format(version, index_name)
            return prefix

        indices = {k: get_prefix(v) for k, v in self.index_names.items()
                   if v is not None}
        return indices

