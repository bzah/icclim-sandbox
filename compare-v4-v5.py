import glob

import numpy as np
from matplotlib import pyplot as plt

from icclim.eca_indices import SNOW_GROUP
import eca_indices
import xarray as xr

CLIMP_PATH = "/Users/aoun/workspace/climpact-master/www/output/gridded/"
V5_PATH = "/Users/aoun/workspace/icclim/netcdf_files/output/"
V4_PATH = "/Users/aoun/workspace/secondary-wt/netcdf_files/output/"
FREQ = "ANN"


def convert_unit(da, var_name: str):
    if var_name in ["HD17", "TX", "TN", "DTR", "VDTR", "ETR", "GD4"]:
        # hd17 for v4 (unit should be "K days" but is K)
        # tx, tn for v4 (unit should be °C but is K)
        # dtr, etd, vdtr are kind of unit less (diff of temperatures) so it should not be converted
        # GD4 unit is improperly set to K in v4
        return da
    if da.attrs.get("units", None) == "1":
        return da * 100
    if da.attrs.get("units", None) == "K":
        return da - 273.15
    if da.dtype == np.dtype("timedelta64[ns]"):
        return da / np.timedelta64(1, "D")
    return da


def run():
    for ind in eca_indices.Indice:
        plot_rows = 1
        print(ind.name)
        v5_ds = xr.open_dataset(
                f"{V5_PATH}{ind.name}_{FREQ}_icclimv5_climpSampleData_1991_2010.nc"
        )
        # PERCENTILES
        v5 = v5_ds[ind.name]
        v5 = convert_unit(v5, ind.name)
        v5_mean = v5.mean(dim="lat", keep_attrs=True).mean(dim="lon", keep_attrs=True)
        v5_pixel = v5[:, 0, 0]
        v5_per = v5_ds.get("percentiles", None)
        if v5_per is not None:
            v5_per_mean = v5_per.mean(dim="lat", keep_attrs=True).mean(dim="lon",
                                                                       keep_attrs=True)
            v5_per_pixel = v5_per.isel(lat=0).isel(lon=0)
            plot_rows = 2
        try:
            v4 = xr.open_dataset(
                    f"{V4_PATH}{ind.name}_{FREQ}_icclimv4_climpSampleData_1991_2010.nc"
            )[ind.name]
            v4 = convert_unit(v4, ind.name)
            v4_mean = v4.mean(dim="lat", keep_attrs=True).mean(dim="lon",
                                                               keep_attrs=True)
            v4_pixel = v4[:, 0, 0]
            v4_has_index = True
        except Exception:
            # v4 is broken with "CD", "CW", "WW", "WD"
            v4_has_index = False
        try:
            climpact_files = glob.glob(f"{CLIMP_PATH}{ind.indice_name}_{FREQ}*.nc")
            if len(climpact_files) > 1:
                print(f"Climpact has multiple files for {ind.indice_name}."
                      " Taking the first one")
            climpact_result = xr.open_dataset(climpact_files[0])
            climp = convert_unit(climpact_result[ind.indice_name], ind.name)
            climp_mean = climp.mean(dim="lat", keep_attrs=True).mean(dim="lon",
                                                                     keep_attrs=True)
            if v5_per is not None:
                climp_per = xr.open_dataset(
                        glob.glob(f"{CLIMP_PATH}thresholds*.nc")[0]
                ).get(get_climp_thresh(ind.indice_name), None)
                if climp_per is not None:
                    # duplicating 60th value for missing 29Feb
                    climp_per = xr.concat(
                            [climp_per[0:60], climp_per[60], climp_per[60:]],
                            dim="time")
                    climp_per += 273.15
                    climp_per["time"] = np.arange(1, 367)
                    climp_per_mean = climp_per.mean(dim="lat", keep_attrs=True).mean(
                            dim="lon", keep_attrs=True)
                    climp_per_pixel = climp_per.isel(lat=0).isel(lon=0)

            climp_pixel = climp[:, 0, 0]
            climp_has_index = True
        except Exception:
            climp_has_index = False
        # PLOTTING
        fig, axs = plt.subplots(plot_rows, 2, figsize=(20, 10))
        if plot_rows == 1:
            graph_a = 0
            graph_b = 1
        else:
            graph_a = 0, 0
            graph_b = 0, 1
            graph_c = 1, 0
            graph_d = 1, 1
        if v4_has_index and ind not in [eca_indices.Indice.R95PTOT,
                                        eca_indices.Indice.R99PTOT]:
            axs[graph_a].plot(v4.time,
                        v4_mean,
                        linewidth=2,
                        dashes=[6, 2],
                        label="icclim_v4",
                        color="blue")
            axs[graph_b].plot(v4.time, v4_pixel,
                        linewidth=2,
                        dashes=[6, 2],
                        label="icclim_v4",
                        color="blue")
        if climp_has_index and ind not in [eca_indices.Indice.R95P,
                                           eca_indices.Indice.R99P]:
            axs[graph_a].plot(climp.time,
                        climp_mean,
                        linewidth=2,
                        label="climpact",
                        color="green")
            axs[graph_b].plot(climp.time,
                        climp_pixel,
                        linewidth=2,
                        label="climpact",
                        color="green")
        axs[graph_a].plot(v5.time, v5_mean, linewidth=1, label="icclim_v5", color="black")
        axs[graph_b].plot(v5.time, v5_pixel, linewidth=1, label="icclim_v5", color="black")
        axs[graph_b].legend(prop={"size": 10})
        axs[graph_a].legend(prop={"size": 10})
        axs[graph_a].set_title("Mean on lat and lon")
        pixel_name = f"lat:{format(v5_pixel.lat.values[()], '.3f')} lon:{format(v5_pixel.lon.values[()], '.3f')}"
        axs[graph_b].set_title(                f"Values at {pixel_name}")
        unit = v5_pixel.attrs.get("units", None)
        if unit is None:
            print("\tNo units!")
        else:
            axs[graph_b].set_ylabel(unit)
            axs[graph_a].set_ylabel(unit)
        if v5_per is not None:
            if v5_per.coords.get("dayofyear", None) is not None:
                # percentiles plots
                if climp_per is not None:
                    axs[graph_c].plot(climp_per_mean.time, climp_per_mean, linewidth=2,
                                label="climpact", color="green")
                    axs[graph_d].plot(climp_per_pixel.time, climp_per_pixel, linewidth=2,
                                label="climpact", color="green")
                axs[graph_c].plot(v5_per_mean.dayofyear, v5_per_mean, linewidth=1,
                            label="icclim_v5",
                            color="black")
                axs[graph_d].plot(v5_per_pixel.dayofyear, v5_per_pixel, linewidth=1,
                            label="icclim_v5",
                            color="black")
                axs[graph_c].set_ylabel("K")
                axs[graph_c].set_xlabel("day of year")
                axs[graph_c].legend(prop={"size": 10})
                axs[graph_c].set_title(
                    f"Percentiles - mean on lat and lon - period {v5_per.climatology_bounds}")
                axs[graph_d].set_ylabel("K")
                axs[graph_d].set_xlabel("day of year")
                axs[graph_d].legend(prop={"size": 10})
                axs[graph_d].set_title(
                    f"Percentiles - at {pixel_name} - period {v5_per.climatology_bounds}")
            else:
                if climp_per is not None:
                    axs[graph_c].plot(climp_per.lat, climp_per.mean(dim="lon"), linewidth=2,
                                label="climpact", color="green")
                    axs[graph_d].plot(climp_per.lat, climp_per.isel(lon=0), linewidth=2,
                                label="climpact", color="green")
                axs[graph_c].plot(v5_per.lat, v5_per.mean(dim="lon"), linewidth=1,
                                  label="icclim_v5",
                                  color="black")
                axs[graph_d].plot(v5_per.lat, v5_per.isel(lon=0), linewidth=1,
                                  label="icclim_v5",
                                  color="black")
        plt.suptitle(ind.indice_name)
        plt.savefig(f"comparison-abel-/v5-vs-v4_ANN/{ind.indice_name}.png")
        plt.close(fig)


def get_climp_thresh(indice_name):
    prefix = indice_name[0:-1]
    if indice_name == "wsdi":
        prefix = "tx90"
    elif indice_name == "csdi":
        prefix = "tn10"
    return f"{prefix}thresh"


if __name__ == "__main__":
    run()
