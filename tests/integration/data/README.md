# Integration Test Data
As we update the dictionary and its downstream gdc-models, it is important that we keep the in the test up to date with the structures in the mappings contained in gdc-models. Hence, this CLI exposes functions for ensuing this.

### Contents
- [Remove Vestigial Data](#remove-vestigial-data)

## Remove Vestigial Data
As fields are moved to vestigial, the data associated with them is no longer included in the graph indices & therefor no longer included in the viz indices. This script removes these vestigial data points from our input data for both case & file in order to ensure our tests continue to function properly.

Command:
```bash
python -m tests.integration.data remove-vestigial
```
