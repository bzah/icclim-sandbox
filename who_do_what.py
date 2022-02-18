"""
Script to compare indices provided by xclim vs the ones of clix-meta.
The output is printed in stdout.
Needs icclim and xclim to be installed.

Run it with `python -m who_do_what`
"""
import xclim.indicators.seaIce
import xclim.indicators.atmos
import xclim.indicators.land

from icclim.clix_meta.clix_meta_indices import ClixMetaIndices

def get_xclim_index_name(module):
    acc = []
    for a in list(filter(lambda x: x[0] != "_", dir(module))):
        acc.append(
            (a,
             module.__dict__[a]
             .cf_attrs[0]
             .get("standard_name", "no_standard_name")))
    return acc


def main():
    xclim_indices = []
    xclim_indices += get_xclim_index_name(xclim.indicators.seaIce)
    xclim_indices += get_xclim_index_name(xclim.indicators.atmos)
    xclim_indices += get_xclim_index_name(xclim.indicators.land)
    clix_meta = ClixMetaIndices.get_instance()
    clix_indices = list(map(lambda x: (x, clix_meta.indices_record[x]["output"]["standard_name"]),
                            clix_meta.indices_record.keys()))
    in_xclim_not_in_clix = []
    in_clix_not_in_xclim = []
    in_both = []
    for xc in xclim_indices:
        if xc[1] != "no_standard_name":
            if xc[1].upper() not in list(
                    map(lambda clix: str(clix[1]).upper(), clix_indices)):
                in_xclim_not_in_clix.append(xc)
            else:
                in_both.append((xc, "by standard_name"))
        else:
            if xc[0].upper() not in list(
                    map(lambda clix: str(clix[0]).upper(), clix_indices)):
                in_xclim_not_in_clix.append(xc)
            else:
                in_both.append((xc, "by short name"))
    for clix in clix_indices:
        if clix[1] != "no_standard_name":
            if str(clix[1]).upper() not in list(
                    map(lambda ind: str(ind[1]).upper(), xclim_indices)):
                in_clix_not_in_xclim.append(clix)
        else:
            if str(clix[0]).upper() not in list(
                    map(lambda ind: str(ind[0]).upper(), xclim_indices)):
                in_clix_not_in_xclim.append(clix)
    # TODO: Improve output to create a csv or something similar
    print(f"in_xclim_not_in_clix indices: {in_xclim_not_in_clix}")
    print(f"in_clix_not_in_xclim indices: {in_clix_not_in_xclim}")
    print(f"in_both indices: {in_both}")


if __name__ == '__main__':
    main()
