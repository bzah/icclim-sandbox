import logging
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Type, Union

from xarray.core.dataset import Dataset
import xarray as xr


def netcdf_processing():

    ds1 = xr.open_dataset("xclim_tx90-base-period-91-00.nc")
    ds2 = xr.open_dataset("ds2-fixed.nc")
    nc_variable = "tx90p"
    ds2[nc_variable] = ds2[nc_variable].astype(ds1[nc_variable].dtype)
    diff = (ds1[nc_variable] - ds2[nc_variable]) / ds1[nc_variable] * 100

    global_mean = float(diff.mean())
    print(f"The average difference is {global_mean}%")
    time_mean = diff.mean(dim="time")

    can_compare_content = compare_metadata(ds1, ds2)
    # compare_content(ds1_out, icc_out)


def compare_metadata(ds1: Dataset, ds2: Dataset) -> bool:
    # TODO use cf-metadata as a base for comparison
    can_compare_content = True
    logging.info("\n# Metadata comparison")
    if ds1.dims != ds2.dims:
        can_compare_content = False
        logging.info("dims are diffrents !")
    compare_attributes(ds1, ds2)
    compare_variables(ds1, ds2)
    return can_compare_content


def compare_variables(ds1, ds2):
    def name_it(x):
        return x["standard_name"]

    ds1_vars = get_variable_properties(ds1, "standard_name")
    ds2_vars = get_variable_properties(ds2, "standard_name")
    _vars = get_variable_properties(ds1, "standard_name")
    logging.info("\n original dataset has the following variables :")
    logging.info(list(map(name_it, _vars)))
    logging.info("\n ds1 dataset has the following variables :")
    logging.info(list(map(name_it, ds1_vars)))
    logging.info("\n ds2 dataset has the following variables :")
    logging.info(list(map(name_it, ds2_vars)))


def compare_attributes(ds1, ds2):
    logging.info(" ds1 ds has %d attrs", len(ds1.attrs))
    logging.info(" ds2 ds has %d attrs", len(ds2.attrs))
    ds1_added_attrs = filter(lambda x: ds2.attrs.get(x) == None, ds1.attrs)
    logging.info("\n ds1 added the following attributs")
    logging.info(list(ds1_added_attrs))
    ds2_added_attrs = filter(lambda x: ds1.attrs.get(x) == None, ds2.attrs)
    logging.info("\n ds2 added the following attributs")
    logging.info(list(ds2_added_attrs))


def compare_content(ds1: Dataset, ds2: Dataset):
    logging.info("\n# Content comparison")
    common_variables = (
        variable for variable in list(ds1.keys()) if variable in list(ds2.keys())
    )
    logging.info("Common variables comparison")
    for variable in common_variables:
        logging.info("**%s**", variable)
        if ds1[variable].size == ds2[variable].size:
            logging.info("same size")
        else:
            logging.info(
                "size is different, %s for ds1 and %s for ds2",
                ds1[variable].size,
                ds2[variable].size,
            )
    logging.info("Common coordinates comparison")
    common_coords = (
        variable for variable in list(ds1.coords) if variable in list(ds2.coords)
    )
    for coord in common_coords:
        logging.info("**%s**", coord)
        if ds1[coord].size == ds2[coord].size:
            logging.info("same size")
        else:
            logging.info(
                "size is different, %s for ds1 and %s for ds2",
                ds1[coord].size,
                ds2[coord].size,
            )
    # TODO, compare dims and compare unique variables/coords


def get_variable_properties(ds: Dataset, property_name: str):
    return list(
        variable.attrs
        for variable in list(ds.variables.values())
        if property_name in variable.attrs
    )


if __name__ == "__main__":
    netcdf_processing()
