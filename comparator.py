from xarray.core.dataarray import DataArray

from xarray.core.dataset import Dataset
import xarray as xr


def netcdf_processing():

    icclim = "icclim_FIXED.nc"
    xclim = "xclim_tx90-base-period-91-00.nc"
    climpact = "climpact_tx90p_FIXED.nc"

    file1 = xclim
    file2 = climpact
    ds1 = xr.open_dataset(file1, decode_timedelta=False)
    ds2 = xr.open_dataset(file2, decode_timedelta=False)
    nc_variable = "tx90p"
    print(f"# Compararison of {file1} and {file2} through {nc_variable} variable")
    print(f"*In the following, {file1} will be called ds1 and {file2} will be ds2*")
    ds1[nc_variable] = ds1[nc_variable].astype("float64")
    ds2[nc_variable] = ds2[nc_variable].astype("float64")
    ds1[nc_variable] = convert_unit(ds1[nc_variable])
    ds2[nc_variable] = convert_unit(ds2[nc_variable])

    can_compare_content = compare_metadata(ds1, ds2)
    if not can_compare_content:
        print("Content are too different and cannot be compared further")
        return

    compare_by_mean(ds1, ds2, nc_variable)
    compare_by_max(ds1, ds2, nc_variable)
    compare_by_min(ds1, ds2, nc_variable)
    # compare_content(ds1_out, icc_out)


def compare_by_mean(ds1, ds2, nc_variable):
    ds1_mean_time = ds1[nc_variable].mean(dim="time")
    ds2_mean_time = ds2[nc_variable].mean(dim="time")
    diff = (ds1_mean_time - ds2_mean_time) / ds1_mean_time * 100
    print("\n## Comparison of datasets *averaged* on time")
    print(
        f"In average, ds1 and ds2 averages are {float(diff.mean())} % of ds1 different"
    )
    print(f"The max difference is {float(diff.max()) } % of ds1")
    print(f"The min difference is {float(diff.min()) } % of ds1")


def compare_by_max(ds1, ds2, nc_variable):
    ds1_max_time = ds1[nc_variable].max(dim="time")
    ds2_mean_time = ds2[nc_variable].max(dim="time")
    diff = (ds1_max_time - ds2_mean_time) / ds1_max_time * 100
    print("\n## Comparison of datasets *maximums* on time")
    print(
        f"In average, ds1 and ds2 maximums are {float(diff.mean())} % of ds1 different"
    )
    print(f"The max difference is {float(diff.max()) } % of ds1")
    print(f"The min difference is {float(diff.min()) } % of ds1")


def compare_by_min(ds1, ds2, nc_variable):
    ds1_min_time = ds1[nc_variable].min(dim="time")
    ds2_mean_time = ds2[nc_variable].min(dim="time")
    diff = (ds1_min_time - ds2_mean_time) / ds1_min_time * 100
    print("\n## Comparison of datasets *minimum* on time")
    print("**Minimum is irrelevant if the studied variable units minimum is zero**")
    print(
        f"In average, ds1 and ds2 minimums are {float(diff.mean())} % of ds1 different"
    )
    print(f"The max difference is {float(diff.max()) } % of ds1")
    print(f"The min difference is {float(diff.min()) } % of ds1")


def convert_unit(da: DataArray):
    if da.units == "%":
        # FIXME Might be too inacurate to multiply by 30 but it will laborious to choose between 31, 29 and 30
        return da * 30 / 100
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
    print(" ds1 ds has %d attrs", len(ds1.attrs))
    print(" ds2 ds has %d attrs", len(ds2.attrs))
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
