import datetime
import time

from distributed import Client

import icclim.main
import xarray as xr

from icclim.models.constants import TAS, SNOW_GROUP, TAS_MAX
from icclim.models.ecad_indices import EcadIndex


def run():
    time_start = time.perf_counter()
    Client(memory_limit="12GB", timeout=20, n_workers=1, threads_per_worker=10)
    bp = [datetime.datetime(1991, 1, 1), datetime.datetime(2000, 12, 31)]
    tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2010, 12, 31)]
    res = []
    ds = xr.open_dataset("netcdf_files/climpact.full.nc")
    origin_unit = ds.precip.attrs["units"]

    tas = filter(lambda x: TAS in x.variables, EcadIndex)
    for ind in tas:
        out_unit = None
        if ind.name.find("P") != -1:
            out_unit = "%"
        print(ind)
        if ind.group == SNOW_GROUP:
            ds.precip.attrs["units"] = "mm"
        else:
            ds.precip.attrs["units"] = origin_unit
        res.append(
            (
                ind.name,
                icclim.main.index(
                    index_name=ind.name,
                    in_files=ds,
                    slice_mode="YS",
                    base_period_time_range=bp,
                    time_range=tr,
                    transfer_limit_Mbytes=256,
                    out_unit=out_unit,
                    save_percentile=True,
                ),
            )
        )
    for r in res:
        r[1].to_netcdf(
            f"netcdf_files/output/{r[0]}_ANN_icclimv5_climpSampleData_1991_2010.nc",
            encoding={"time": {"units": "days since 1850-1-1"}},
        )
    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")


if __name__ == "__main__":
    run()
