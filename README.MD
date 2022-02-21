# Comparison project between the climate indices computation libraries

Its purpose is to validate the results of icclim with Climdex/Climpact.

A few scripts are provided:
- `netcdf-comparator.py` (WIP) compares the content of netcdf files (will likely be deleted).
- `compare-v5-v4-climp.py` uses matplotlib to compare the result of icclimv5 icclimv4 and climpact. 
All indices should be computed before starting this script.
- `icclim-v5-sandbox.py` runs icclim index individually, meant to support icclim development.
- `produce_all_indices_v5.py` runs all icclim-v5 49 indices. Can be used before compare-v5-v4-climp.py to
build all the indices.
- `produce_all_indices_v4.py` same but for icclim-v4 (will be deleted once v5 is release).
This script cannot be run as is, use `produce_all_indices_v5.py` instead.
- `who_do_what.py` compares xclim indicators and clix-meta indices.

A report is also available on LaTex format with `compare.tex` file.
To compile this report, you must first run `compare-v5-v4-climp.py` to generate the necessary pictures.