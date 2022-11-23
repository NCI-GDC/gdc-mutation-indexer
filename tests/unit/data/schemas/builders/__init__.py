from tests.unit.data import schemas


class Input(schemas.Schema):
    AGGREGATED_SOMATIC_MUTATION = "maf/aggregated_somatic_mutation.yaml"
    ASCAT = "ascat/input_ascat.yaml"
    ASCAT_ES = "ascat/es_file.json"
    CASE = "case/input_case.yaml"
    CASE_CENTRIC_CASE = "case_centric/input_case.yaml"
    CASE_CENTRIC_SAMPLE = "case_centric/input_sample.yaml"
    CIVIC = "clinical_annotations/civic/input_maf.yaml"
    GENE_MODEL = "gene_model/raw_gene_model.json"
    MAF_FILE_ES = "maf_metadata/es_file.yaml"
    MASKED_SOMATIC_MUTATION = "maf/masked_somatic_mutation.yaml"
    PRIMARY_ALIQUOT_FILE = "primary_aliquot/input_file.json"

    @property
    def _prefix(self) -> str:
        return "builders"


class Final(schemas.Schema):
    ASCAT = "ascat/final_ascat.json"
    CASE = "case/final_case.yaml"
    CASE_CENTRIC = "case_centric/final_case_centric.yaml"
    CIVIC = "clinical_annotations/civic/final_maf.yaml"
    CONSEQUENCE = "consequence/final_consequence.yaml"
    CONSEQUENCE_AA_CHANGE = "consequence/final_consequence_with_aa_change.yaml"
    CONSEQUENCE_GENES = "consequence/final_consequence_with_genes.yaml"
    GENE_MODEL = "gene_model/final_gene_model.json"
    OBSERVATION_CNV = "observation/final_cnv_observation.yaml"
    OBSERVATION_SSM = "observation/final_ssm_observation.json"
    OBSERVATION_SSM_OTHER = "observation/final_other_ssm_observation.json"
    MAF = "maf/final_maf.yaml"
    MAF_METADATA = "maf_metadata/final_maf_metadata.yaml"
    PRIMARY_ALIQUOT = "primary_aliquot/final_primary_aliquot.json"

    @property
    def _prefix(self) -> str:
        return "builders"
