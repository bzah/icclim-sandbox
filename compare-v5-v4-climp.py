"""
Build pretty graphs comparing result from icclim.v4 icclim.v5 (which uses xclim)
and climpact.

Should be used after creating all (or most) of the indices on a simple dataset with the
three library (only icclim v5 is mandatory).
A good example is the climpact sample netcdf.
"""

import glob
from typing import Optional

import numpy as np
from matplotlib import pyplot as plt

import xarray as xr

from icclim.models.ecad_indices import EcadIndex

CLIMP_PATH = "/Users/aoun/workspace/climpact-master/www/output/"
V5_PATH = "/Users/aoun/workspace/icclim/netcdf_files/output/"
V4_PATH = "/Users/aoun/workspace/secondary-wt/netcdf_files/output/"
FREQ = "ANN"

Precips = [EcadIndex.R95P, EcadIndex.R75P, EcadIndex.R99P]
PrecipsTot = [
    EcadIndex.R75PTOT,
    EcadIndex.R95PTOT,
    EcadIndex.R99PTOT,
]

climp_map = {
    "gd4": "gddgrow4",
    "hd17": "hddheat17",
}


class ComparableIndex:
    def __init__(self, ds, da, mean, pixel):
        self.ds = ds
        self.da = da
        self.mean = mean
        self.pixel = pixel


def run():
    # for ind in [EcadIndex.FD]:
    for ind in EcadIndex:
        v4 = None
        v4_per = None
        climp = None
        climp_per = None
        print(ind.name)
        v5 = build_comparable(
            ind, f"{V5_PATH}{ind.name}_{FREQ}_icclimv5_climpSampleData_1991_2010.nc"
        )
        pixel = v5.da.max("time").stack(stacked=["lat", "lon"]).argmax()
        v5.pixel = v5.da.stack(stacked=["lat", "lon"]).isel(stacked=pixel)
        if ind not in PrecipsTot:
            # v4 use another formula for rxxptot
            v4 = build_comparable(
                ind,
                f"{V4_PATH}{ind.index_name}_{FREQ}_icclimv4_climpSampleData_1991_2010.nc",
                lambda x: ind.index_name,
                pixel,
            )
        if ind not in Precips:
            # climp use another units or rxxp (mm)
            if ind.index_name.lower() in climp_map.keys():
                index = climp_map[ind.index_name.lower()]
            else:
                index = ind.index_name.lower()
            climp = build_comparable(
                ind, f"{CLIMP_PATH}{index}_{FREQ}*.nc", lambda x: index, pixel
            )
        # --- PERCENTILES
        v5_per = get_v5_per(v5)
        if v5_per is not None:
            climp_per = get_climp_per(ind)
            v4_per = get_v4_per(ind)

        # --- PLOTTING
        pixel_name = (
            f"lat:{format(v5.pixel.stacked.values[()][0], '.3f')} "
            f"lon:{format(v5.pixel.stacked.values[()][1], '.3f')}"
        )
        if v5_per is None:
            fig, axs = plt.subplots(1, 2, figsize=(20, 10))
            plot_index(
                axs=axs,
                climp=climp,
                graph_a=0,
                graph_b=1,
                v4=v4,
                v5=v5,
                pixel_name=pixel_name,
            )
        else:
            fig, axs = plt.subplots(2, 2, figsize=(20, 10))
            plot_index(
                axs=axs,
                climp=climp,
                graph_a=(0, 0),
                graph_b=(0, 1),
                v4=v4,
                v5=v5,
                pixel_name=pixel_name,
            )
            plot_percentiles(axs, climp_per, (1, 0), (1, 1), pixel_name, v5_per, v4_per)
        fig.suptitle(ind.index_name)
        fig.savefig(f"comparison/ANN/{ind.index_name}.png")
        plt.close(fig)


def convert_unit(da, var_name: str):
    if var_name.upper() in ["HD17", "TX", "TN", "DTR", "VDTR", "ETR", "GD4"]:
        # hd17 for v4 (unit should be "K days" but is K)
        # tx, tn for v4 (unit should be °C but is K)
        # dtr, etd, vdtr are kind of unit less (diff of temperatures) so they should not be converted
        # GD4 unit is improperly set to K in v4
        return da
    if da.attrs.get("units", None) == "1":
        return da * 100
    if da.attrs.get("units", None) == "K":
        return da - 273.15
    if da.dtype == np.dtype("timedelta64[ns]"):
        return da / np.timedelta64(1, "D")
    return da


def build_comparable(index: EcadIndex, path, trans=lambda x: x, pixel=None):
    try:
        ds = xr.open_dataset(glob.glob(path)[0])
        da = ds[trans(index.index_name)]
        da = convert_unit(da, index.name)
        da_mean = da.mean(dim="lat", keep_attrs=True).mean(dim="lon", keep_attrs=True)
        if pixel is not None:
            da_pixel = da.stack(stacked=["lat", "lon"]).isel(stacked=pixel)
        else:
            da_pixel = da.isel(lat=0).isel(lon=0)
        return ComparableIndex(ds, da, da_mean, da_pixel)
    except Exception:
        return None


def get_climp_per(ind):
    if ind in PrecipsTot:
        val = ind.index_name.lower()[:-3]
    else:
        val = ind.index_name.lower()
    climp_per = xr.open_dataset(glob.glob(f"{CLIMP_PATH}thresholds*.nc")[0]).get(
        get_climp_thresh(val), None
    )
    if climp_per is not None:
        if ind not in Precips and ind not in PrecipsTot:
            # duplicating 60th value for missing 29Feb
            climp_per = xr.concat(
                [climp_per[0:60], climp_per[60], climp_per[60:]], dim="time"
            )
            climp_per += 273.15
            climp_per["time"] = np.arange(1, 367)
            climp_per_mean = climp_per.mean(dim="lat", keep_attrs=True).mean(
                dim="lon", keep_attrs=True
            )
            climp_per_pixel = climp_per.isel(lat=0).isel(lon=0)
            return ComparableIndex(None, climp_per, climp_per_mean, climp_per_pixel)
        return ComparableIndex(None, climp_per, None, None)
    return None


def get_v4_per(ind):
    v4_per = xr.open_dataset(
        glob.glob(
            f"{V4_PATH}{ind.index_name}_{FREQ}_icclimv4_climpSampleData_1991_2010_percentile_array.nc"
        )[0]
    ).get("percentiles", None)
    if v4_per is not None:
        if ind not in Precips and ind not in PrecipsTot:
            # duplicating 60th value for missing 29Feb
            v4_per = xr.concat([v4_per[0:60], v4_per[60], v4_per[60:]], dim="dayofyear")
            v4_per["dayofyear"] = np.arange(1, 367)
            v4_per_mean = v4_per.mean(dim="lat", keep_attrs=True).mean(
                dim="lon", keep_attrs=True
            )
            climp_per_pixel = v4_per.isel(lat=0).isel(lon=0)
            return ComparableIndex(None, v4_per, v4_per_mean, climp_per_pixel)
        return ComparableIndex(None, v4_per, None, None)
    return None


def get_v5_per(v5):
    v5_per = v5.ds.get("percentiles", None)
    if v5_per is not None:
        v5_per_mean = v5_per.mean(dim="lat", keep_attrs=True).mean(
            dim="lon", keep_attrs=True
        )
        v5_per_pixel = v5_per.isel(lat=0).isel(lon=0)
        return ComparableIndex(None, v5_per, v5_per_mean, v5_per_pixel)
    return None


def plot_percentiles(
    axs,
    climp_per: ComparableIndex,
    graph_c,
    graph_d,
    pixel_name,
    v5_per: ComparableIndex,
    v4_per: ComparableIndex,
):
    if v5_per.da.coords.get("dayofyear", None) is not None:
        if climp_per is not None:
            axs[graph_c].plot(
                climp_per.mean.time,
                climp_per.mean,
                linewidth=3,
                label="climpact",
                color="green",
            )
            axs[graph_d].plot(
                climp_per.pixel.time,
                climp_per.pixel,
                linewidth=3,
                label="climpact",
                color="green",
            )
        if v4_per is not None:
            axs[graph_c].plot(
                v4_per.mean.dayofyear,
                v4_per.mean,
                linewidth=2,
                label="icclim_v4",
                color="blue",
            )
            axs[graph_d].plot(
                v4_per.pixel.dayofyear,
                v4_per.pixel,
                linewidth=2,
                label="icclim_v4",
                color="blue",
            )
        axs[graph_c].plot(
            v5_per.da.dayofyear,
            v5_per.mean,
            linewidth=1,
            label="icclim_v5",
            color="black",
        )
        axs[graph_d].plot(
            v5_per.da.dayofyear,
            v5_per.pixel,
            linewidth=1,
            label="icclim_v5",
            color="black",
        )
        axs[graph_c].set_ylabel("K")
        axs[graph_c].set_xlabel("day of year")
        axs[graph_c].legend(prop={"size": 10})
        axs[graph_c].set_title(
            f"Percentiles - mean on lat and lon - period {v5_per.da.climatology_bounds}"
        )
        axs[graph_d].set_ylabel("K")
        axs[graph_d].set_xlabel("day of year")
        axs[graph_d].legend(prop={"size": 10})
        axs[graph_d].set_title(
            f"Percentiles - at {pixel_name} - period {v5_per.da.climatology_bounds}"
        )
    else:
        if climp_per is not None:
            axs[graph_c].plot(
                climp_per.da.lat,
                climp_per.da.mean(dim="lon"),
                linewidth=2,
                label="climpact",
                color="green",
            )
            axs[graph_d].plot(
                climp_per.da.lat,
                climp_per.da.isel(lon=0),
                linewidth=2,
                label="climpact",
                color="green",
            )
        if v4_per is not None:
            axs[graph_c].plot(
                v4_per.da.lat,
                v4_per.da.mean(dim="lon"),
                linewidth=2,
                dashes=[6, 2],
                label="icclim_v4",
                color="blue",
            )
            axs[graph_d].plot(
                v4_per.da.lat,
                v4_per.da.isel(lon=0),
                linewidth=2,
                dashes=[6, 2],
                label="icclim_v4",
                color="blue",
            )
        axs[graph_c].plot(
            v5_per.da.lat,
            v5_per.da.mean(dim="lon"),
            linewidth=1,
            label="icclim_v5",
            color="black",
        )
        axs[graph_d].plot(
            v5_per.da.lat,
            v5_per.da.isel(lon=0),
            linewidth=1,
            label="icclim_v5",
            color="black",
        )
        axs[graph_c].set_ylabel("mm")
        axs[graph_c].set_xlabel("lat")
        axs[graph_c].legend(prop={"size": 10})
        axs[graph_c].set_title(f"Percentiles - mean on lon")
        axs[graph_d].set_ylabel("mm")
        axs[graph_d].set_xlabel("lat")
        axs[graph_d].legend(prop={"size": 10})
        axs[graph_d].set_title(
            f"Percentiles - at lon = {format(v5_per.da.lon.values[0], '.3f')}"
        )


def plot_index(
    axs,
    climp: Optional[ComparableIndex],
    graph_a,
    graph_b,
    v4: ComparableIndex,
    v5: ComparableIndex,
    pixel_name,
):
    if v4 is not None:
        axs[graph_a].plot(
            v4.da.time,
            v4.mean,
            linewidth=2,
            dashes=[6, 2],
            label="icclim_v4",
            color="blue",
        )
        axs[graph_b].plot(
            v4.da.time,
            v4.pixel,
            linewidth=2,
            dashes=[6, 2],
            label="icclim_v4",
            color="blue",
        )
    if climp is not None:
        axs[graph_a].plot(
            climp.da.time, climp.mean, linewidth=2, label="climpact", color="green"
        )
        axs[graph_b].plot(
            climp.da.time, climp.pixel, linewidth=2, label="climpact", color="green"
        )
    axs[graph_a].plot(
        v5.da.time, v5.mean, linewidth=1, label="icclim_v5", color="black"
    )
    axs[graph_b].plot(
        v5.da.time, v5.pixel, linewidth=1, label="icclim_v5", color="black"
    )
    axs[graph_b].legend(prop={"size": 10})
    axs[graph_a].legend(prop={"size": 10})
    axs[graph_a].set_title("Mean on lat and lon")
    axs[graph_b].set_title(f"Values at {pixel_name}")
    unit = v5.pixel.attrs.get("units", None)
    if unit is None:
        print("\tNo units!")
    else:
        axs[graph_b].set_ylabel(unit)
        axs[graph_a].set_ylabel(unit)


def get_climp_thresh(index_name):
    prefix = index_name[0:-1]
    if index_name == "wsdi":
        prefix = "tx90"
    elif index_name == "csdi":
        prefix = "tn10"
    return f"{prefix}thresh"


if __name__ == "__main__":
    run()
