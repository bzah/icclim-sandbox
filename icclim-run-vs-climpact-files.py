import datetime
import glob
import time

import eca_indices
import numpy as np
import xarray as xr
from distributed import Client
from matplotlib import pyplot as plt

import icclim


def convert_unit(da, var_name: str):
    if var_name in ["dtr"]:
        return da
    if da.attrs.get("units", None) == "degrees_C":
        return da + 273.15
    if da.dtype == np.dtype("timedelta64[ns]"):
        return da / np.timedelta64(1, "D")
    return da


def run():
    time_start = time.perf_counter()

    files = "netcdf_files/climpact.sampledata.gridded.1991-2010.nc"

    Client(memory_limit="8GB", timeout=20, n_workers=1, threads_per_worker=8)
    bp = [datetime.datetime(1991, 1, 1), datetime.datetime(2000, 12, 31)]
    tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2025, 12, 31)]

    # Attempt to call R code, does not work because path of inner climdex library is unknown
    # import rpy2.robjects as robjects
    # r = robjects.r
    # r['source'](
    #     '/Users/aoun/workspace/climpact-master/climpact.ncdf.wrapper.r')
    # calc_indice_r = robjects.globalenv['calc_indice']

    filtered = eca_indices.Indice
    # filtered = [x for x in filtered if x.indice_name == "tg10p"]
    for ind in filtered:
        print(ind.indice_name)
        out_f = f"netcdf_files/output/icclim_{ind.indice_name}_1991_2100.nc"
        try:
            climpact_files = glob.glob(
                f"/Users/aoun/workspace/climpact-master/www/output/gridded/{ind.indice_name}_MON*.nc"
            )
            if len(climpact_files) > 1:
                print(
                    f"Climpact has multiple files for {ind.indice_name}. Taking the first one"
                )
            climpact_result = xr.open_dataset(climpact_files[0])
        except Exception:
            print(ind.indice_name + " ignored")
            # ignore this indice
            continue
        out_unit = None
        if ind.indice_name.find("p") != -1:
            out_unit = "%"
        icclim_result = icclim.indice(
            indice_name=ind.indice_name,
            in_files=files,
            slice_mode="MS",
            base_period_time_range=bp,
            time_range=tr,
            out_file=out_f,
            transfer_limit_Mbytes=200,
            out_unit=out_unit,
        )
        icc_ind = icclim_result[ind.indice_name]
        climp_ind = convert_unit(climpact_result[ind.indice_name], ind.indice_name)

        icc = icc_ind.mean(dim="lat").mean(dim="lon")
        climp = climp_ind.mean(dim="lat").mean(dim="lon")

        fig, ax = plt.subplots(1, 1, figsize=(20, 10))
        ax.plot(climp.time, climp, linewidth=2, label="climpact")
        ax.plot(icc.time, icc, linewidth=2, label="icclim")
        plt.legend(prop={"size": 10})
        plt.title(ind.indice_name)
        plt.savefig(f"comparison/{ind.indice_name}.png")

    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")


if __name__ == "__main__":
    run()
