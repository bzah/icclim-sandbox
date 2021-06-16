from icclim import icclim
from icclim.util import read
import logging
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Type, Union

from xarray.core.dataset import Dataset
import xarray as xr
from xclim.core.calendar import percentile_doy

from xclim.indicators import icclim as xicc
from icclim import icclim


# Comparator between icclim 4.x and xclim
def netcdf_processing():

    ds1 = xr.open_dataset("australie-tx90-no-bs.nc")
    ds2 = xr.open_dataset("australie-tx90-no-bs.nc")
    nc_variable = "tx90p"
    diff = (ds1[nc_variable] - ds2[nc_variable]) / ds1[nc_variable] * 100

    global_mean = float(diff.mean())
    print(f"The average difference is {global_mean}%")
    time_mean = diff.mean(dim="time")

    # compare_metadata(ds, xclim_out, icc_out)
    # compare_content(xclim_out, icc_out)


def compare_metadata(
    original_ds: xr.Dataset, xclim_output_ds: Dataset, icclim_output_ds: Dataset
):
    xlog.info("\n# Metadata comparison")
    compare_attributes(original_ds, xclim_output_ds, icclim_output_ds)
    compare_variables(xclim_output_ds, icclim_output_ds, original_ds)


def compare_variables(xclim_output_ds, icclim_output_ds, original_ds):
    def name_it(x):
        return x["standard_name"]

    xclim_vars = get_variable_properties(xclim_output_ds, "standard_name")
    icclim_vars = get_variable_properties(icclim_output_ds, "standard_name")
    original_ds_vars = get_variable_properties(original_ds, "standard_name")
    xlog.info("\n original dataset has the following variables :")
    xlog.info(list(map(name_it, original_ds_vars)))
    xlog.info("\n xclim dataset has the following variables :")
    xlog.info(list(map(name_it, xclim_vars)))
    xlog.info("\n icclim dataset has the following variables :")
    xlog.info(list(map(name_it, icclim_vars)))


def compare_attributes(original_ds, xclim_output_ds, icclim_output_ds):
    xlog.info(" Original ds has %d attrs", len(original_ds.attrs))
    xlog.info(" Xclim ds has %d attrs", len(xclim_output_ds.attrs))
    xlog.info(" Icclim ds has %d attrs", len(icclim_output_ds.attrs))
    xclim_added_attrs = filter(
        lambda x: original_ds.attrs.get(x) == None, xclim_output_ds.attrs
    )
    xlog.info("\n Xclim added the following attributs")
    xlog.info(list(xclim_added_attrs))
    icclim_added_attrs = filter(
        lambda x: original_ds.attrs.get(x) == None, icclim_output_ds.attrs
    )
    xlog.info("\n Icclim added the following attributs")
    xlog.info(list(icclim_added_attrs))


def compare_content(xclim_output_ds: Dataset, icclim_output_ds: Dataset):
    xlog.info("\n# Content comparison")
    common_variables = (
        variable
        for variable in list(xclim_output_ds.keys())
        if variable in list(icclim_output_ds.keys())
    )
    xlog.info("Common variables comparison")
    for variable in common_variables:
        xlog.info("**%s**", variable)
        if xclim_output_ds[variable].size == icclim_output_ds[variable].size:
            xlog.info("same size")
        else:
            xlog.info(
                "size is different, %s for xclim and %s for icclim",
                xclim_output_ds[variable].size,
                icclim_output_ds[variable].size,
            )
    xlog.info("Common coordinates comparison")
    common_coords = (
        variable
        for variable in list(xclim_output_ds.coords)
        if variable in list(icclim_output_ds.coords)
    )
    for coord in common_coords:
        xlog.info("**%s**", coord)
        if xclim_output_ds[coord].size == icclim_output_ds[coord].size:
            xlog.info("same size")
        else:
            xlog.info(
                "size is different, %s for xclim and %s for icclim",
                xclim_output_ds[coord].size,
                icclim_output_ds[coord].size,
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
