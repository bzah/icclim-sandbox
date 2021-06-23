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

    compare_netcdf_files(xclim_per_90, climpact_per_90)
    print("----------------------------")
    compare_netcdf_files(icclim_per_90, climpact_per_90)
    print("----------------------------")
    compare_netcdf_files(icclim_per_90, xclim_per_90)

    # compare_netcdf_files(tx90_xclim_new, tx90_climpact)
    # print("----------------------------")
    # compare_netcdf_files(tx90_xclim, tx90_climpact)
    # print("----------------------------")
    # compare_netcdf_files(tx90_xclim, tx90_xclim_new)


def compare_netcdf_files(file1: NetcdfFile, file2: NetcdfFile):
    file1.ds = xr.open_dataset(file1.path, decode_timedelta=False)
    file2.ds = xr.open_dataset(file2.path, decode_timedelta=False)
    print(
        f"# Compararison of **{file1.name}** and **{file2.name}** through {file1.variable} and {file2.variable} variables"
    )
    file1.ds[file1.variable] = file1.ds[file1.variable].astype("float64")
    file2.ds[file2.variable] = file2.ds[file2.variable].astype("float64")
    file1.ds[file1.variable] = convert_unit(file1.ds[file1.variable])
    file2.ds[file2.variable] = convert_unit(file2.ds[file2.variable])

    # can_compare_content = compare_metadata(file1.ds, file2.ds)
    # if not can_compare_content:
    #     print("Content are too different and cannot be compared further")
    #     return

    compare_by_stats(file1, file2)
    compare_by_mean(file1, file2)
    compare_by_max(file1, file2)
    # compare_by_min(file1, file2)
    # compare_content(ds1_out, icc_out)


def compare_by_stats(file1: NetcdfFile, file2: NetcdfFile):
    file1_mean = file1.ds[file1.variable].mean()
    file2_mean = file2.ds[file2.variable].mean()
    file1_max = file1.ds[file1.variable].max()
    file2_max = file2.ds[file2.variable].max()
    file1_min = file1.ds[file1.variable].min()
    file2_min = file2.ds[file2.variable].min()
    print("\n## Comparison of datasets main stats")
    print(f"Mean of {file1.name}: {float(file1_mean) }")
    print(f"Mean of {file2.name}: {float(file2_mean) }")
    print(f"Max of {file1.name}: {float(file1_max) }")
    print(f"Max of {file2.name}: {float(file2_max) }")
    print(f"Min of {file1.name}: {float(file1_min) }")
    print(f"Min of {file2.name}: {float(file2_min) }")


def compare_by_mean(file1: NetcdfFile, file2: NetcdfFile):
    ds1_mean_time = file1.ds[file1.variable].mean(dim=file1.dim_to_compare)
    ds2_mean_time = file2.ds[file2.variable].mean(dim=file2.dim_to_compare)
    diff = ds1_mean_time - ds2_mean_time
    print(
        f"\n## Comparison of datasets *averaged* on {file1.dim_to_compare}/{file2.dim_to_compare}"
    )
    print(
        f"diff = {file1.name}.mean('{file1.dim_to_compare}') - {file2.name}.mean('{file2.dim_to_compare}')"
    )
    print(f"In average, _diff.mean()_, the difference is {float(diff.mean())}")
    print(f"For max difference, _diff.max()_ is {float(diff.max()) }")
    print(f"For min difference, _diff.min()_ is {float(diff.min()) }")


def compare_by_max(file1: NetcdfFile, file2: NetcdfFile):
    ds1_max_time = file1.ds[file1.variable].max(dim=file1.dim_to_compare)
    ds2_mean_time = file2.ds[file2.variable].max(dim=file2.dim_to_compare)
    diff = ds1_max_time - ds2_mean_time
    print(
        f"\n## Comparison of datasets *maximums* on {file1.dim_to_compare}/{file2.dim_to_compare}"
    )
    print(
        f"diff = {file1.name}.max('{file1.dim_to_compare}') - {file2.name}.max('{file2.dim_to_compare}')"
    )
    print(f"In average, _diff.mean()_, the difference is{float(diff.mean())} different")
    print(f"For max difference, _diff.max()_ is {float(diff.max()) }")
    print(f"For min difference, _diff.min()_ is {float(diff.min()) }")


# def compare_by_min(ds1, ds2, variable):
#     ds1_min_time = ds1[nc_variable].min(dim="time")
#     ds2_mean_time = ds2[nc_variable].min(dim="time")
#     diff = (ds1_min_time - ds2_mean_time)
#     print("\n## Comparison of datasets *minimum* on time")
#     print("**Minimum is irrelevant if the studied variable units minimum is zero**")
#     print(
#         f"In average, ds1 and ds2 minimums are {float(diff.mean())} different"
#     )
#     print(f"The max difference is {float(diff.max()) }")
#     print(f"The min difference is {float(diff.min()) }")


def convert_unit(da: DataArray):
    # No fun with leap years were attempted here
    coef = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if da.units == "%":
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
