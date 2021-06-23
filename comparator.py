from typing import Callable, List
from xarray.core.dataarray import DataArray
from dataclasses import dataclass

from xarray.core.dataset import Dataset
import xarray as xr


@dataclass
class NetcdfFile:
    name: str
    path: str
    variable: str
    ds: Dataset = None
    dim_to_compare: str = "time"


def netcdf_processing():

    # TODO extract NetcdfFile.s configs to yaml
    tx90_icclim = NetcdfFile(
        name="icclim", path="tx90p/icclim_FIXED.nc", variable="tx90p"
    )
    tx90_xclim = NetcdfFile(
        name="xclim", path="tx90p/xclim_tx90-base-period-91-00.nc", variable="tx90p"
    )
    tx90_xclim_new = NetcdfFile(
        name="xclim_new",
        path="tx90p/new_xclim_tx90-base-period-91-00.nc",
        variable="tx90p",
    )
    tx90_climpact = NetcdfFile(
        name="climpact", path="tx90p/climpact_tx90p_FIXED.nc", variable="tx90p"
    )

    wsdi_icclim = NetcdfFile(name="icclim", path="wsdi/icclim.nc", variable="WSDI")
    wsdi_xclim = NetcdfFile(
        name="xclim", path="wsdi/xclim_6d.nc", variable="warm_spell_duration_index"
    )
    wsdi_climpact = NetcdfFile(
        name="climpact", path="wsdi/climpact.nc", variable="wsdi"
    )

    xclim_per_90 = NetcdfFile(
        name="xclim_per_90",
        path="tx90p/percentiles/xclim.nc",
        variable="tmax",
        dim_to_compare="dayofyear",
    )
    climpact_per_90 = NetcdfFile(
        name="climpact_per_90",
        path="tx90p/percentiles/climpact.nc",
        variable="tx90thresh",
    )
    icclim_per_90 = NetcdfFile(
        name="icclim_per_90", path="tx90p/percentiles/icclim_fixed.nc", variable="Perc",
    )

    # compare_netcdf_files([icclim_per_90, xclim_per_90, climpact_per_90])
    compare_netcdf_files([tx90_icclim, tx90_xclim_new, tx90_climpact])


def compare_netcdf_files(files: List[NetcdfFile]):
    for file in files:
        file.ds = xr.open_dataset(file.path, decode_timedelta=False)
        print(f"{file.name}.{file.variable}")
        file.ds[file.variable] = file.ds[file.variable].astype("float64")
        file.ds[file.variable] = convert_unit(file.ds[file.variable])

    # can_compare_content = compare_metadata(file.ds, file2.ds)
    # if not can_compare_content:
    #     print("Content are too different and cannot be compared further")
    #     return

    compare_by_stats(files)
    compare_by_agregator(
        files,
        "mean",
        lambda netcdf_file: netcdf_file.ds[netcdf_file.variable].mean(
            dim=netcdf_file.dim_to_compare
        ),
    )
    compare_by_agregator(
        files,
        "max",
        lambda netcdf_file: netcdf_file.ds[netcdf_file.variable].max(
            dim=netcdf_file.dim_to_compare
        ),
    )
    # compare_content(ds1_out, icc_out)


def compare_by_stats(files: List[NetcdfFile]):
    print("\n## Comparison of datasets main stats")
    for file in files:
        file_mean = file.ds[file.variable].mean()
        file_max = file.ds[file.variable].max()
        file_min = file.ds[file.variable].min()
        print(f"Mean of {file.name}: {float(file_mean) }")
        print(f"Max of {file.name}: {float(file_max) }")
        print(f"Min of {file.name}: {float(file_min) }")
        print(f"---")


def compare_by_agregator(
    files: List[NetcdfFile], method_name: str, agregator: Callable
):
    print(f"\n## Comparison of datasets by {method_name}")
    files_done = []
    for file1 in files:
        file1_agregation = agregator(file1)
        files_done.append(file1)
        for file2 in files:
            if file2 in files_done:
                continue
            file2_aggregation = agregator(file2)
            diff = file1_agregation - file2_aggregation
            print(
                f"### Comparison of datasets *{method_name}* on {file1.name}.{file1.dim_to_compare} and {file2.name}.{file2.dim_to_compare}\n"
            )
            print(
                f"diff = {file1.name}.{method_name}('{file1.dim_to_compare}') - {file2.name}.{method_name}('{file2.dim_to_compare}')\n->"
            )
            print(f"diff.mean() = {float(diff.mean())}")
            print(f"diff.max() = {float(diff.max()) }")
            print(f"diff.min() = {float(diff.min()) }")
            print("---")


def convert_unit(da: DataArray):
    # TODO: handle leap years ?
    if da.units == "%":
        coef = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        out = []
        groups = da.groupby(da.time.dt.month)
        for month in groups:
            out.append(month[1] * coef[month[0] - 1] / 100)
        return xr.concat(out, dim="time")
    if da.units == "degrees_C":
        return da + 272.15
    return da


def compare_metadata(ds1: Dataset, ds2: Dataset) -> bool:
    # TODO use cf-metadata as a base for comparison
    can_compare_content = True
    print("\n# Metadata comparison")
    if ds1.dims != ds2.dims:
        can_compare_content = False
        print("dims are diffrents !")
    compare_attributes(ds1, ds2)
    compare_variables(ds1, ds2)
    return can_compare_content


def compare_variables(ds1, ds2):
    def name_it(x):
        return x["standard_name"]

    ds1_vars = get_variable_properties(ds1, "standard_name")
    ds2_vars = get_variable_properties(ds2, "standard_name")
    print("\n ds1 dataset has the following variables with standard_name:")
    print(list(map(name_it, ds1_vars)))
    print("\n ds2 dataset has the following variables with standard_name:")
    print(list(map(name_it, ds2_vars)))


def compare_attributes(ds1, ds2):
    print(f" ds1 ds has {len(ds1.attrs)} attrs")
    print(f" ds2 ds has {len(ds2.attrs)} attrs")
    ds1_added_attrs = filter(lambda x: ds2.attrs.get(x) == None, ds1.attrs)
    print("\n ds1 added the following attributs")
    print(list(ds1_added_attrs))
    ds2_added_attrs = filter(lambda x: ds1.attrs.get(x) == None, ds2.attrs)
    print("\n ds2 added the following attributs")
    print(list(ds2_added_attrs))


def compare_content(ds1: Dataset, ds2: Dataset):
    print("\n# Content comparison")
    common_variables = (
        variable for variable in list(ds1.keys()) if variable in list(ds2.keys())
    )
    print("Common variables comparison")
    for variable in common_variables:
        print("**%s**", variable)
        if ds1[variable].size == ds2[variable].size:
            print("same size")
        else:
            print(
                "size is different, %s for ds1 and %s for ds2",
                ds1[variable].size,
                ds2[variable].size,
            )
    print("Common coordinates comparison")
    common_coords = (
        variable for variable in list(ds1.coords) if variable in list(ds2.coords)
    )
    for coord in common_coords:
        print("**%s**", coord)
        if ds1[coord].size == ds2[coord].size:
            print("same size")
        else:
            print(
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
