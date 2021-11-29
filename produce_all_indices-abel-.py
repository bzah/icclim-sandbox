import datetime
import time

from distributed import Client
from icclim.eca_indices import SNOW_GROUP

import icclim.main
import eca_indices
import xarray as xr


def run():
    time_start = time.perf_counter()
    Client(memory_limit='12GB', timeout=20, n_workers=1, threads_per_worker=10)
    files = "netcdf_files/climpact.sampledata.gridded.1991-2010.nc"
    bp = [datetime.datetime(1991, 1, 1), datetime.datetime(2000, 12, 31)]
    tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2010, 12, 31)]
    res = []

    for ind in eca_indices.Indice:
        out_unit = None
        if ind.name.find("P") != -1:
            out_unit = "%"
        print(ind)
        if ind.group == SNOW_GROUP:
            # precip has not a compatible unit for snow
            continue
        res.append((ind.name,
                    icclim.main.indice(
                        indice_name=ind.name,
                        in_files=xr.open_dataset(files),
                        slice_mode="year",
                        base_period_time_range=bp,
                        time_range=tr,
                        out_file="ignored",
                        # transfer_limit_Mbytes=256,
                        out_unit=out_unit,
                        save_percentile=True
                    )))
    for r in res:
        r[1].to_netcdf(
            f"netcdf_files/output/{r[0]}_ANN_icclimv5_climpSampleData_1991_2010.nc")
    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")


if __name__ == "__main__":
    run()
