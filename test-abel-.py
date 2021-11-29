import datetime
import glob
import time

import numpy as np
import pandas as pd

import eca_indices
import icclim
import xarray as xr
from distributed import Client


def run():
    time_start = time.perf_counter()

    # AURELHY
    # out_f = "netcdf_files/output/tx90p_tx_montpe.nc"
    # # client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    # files = "/Users/aoun/workspace/icclim/netcdf_files/yolo_in2.nc"
    # bp = [datetime.datetime(2000, 1, 1), datetime.datetime(2001, 12, 31)]
    # # tr = [datetime.datetime(2000, 1, 1), datetime.datetime(2002, 12, 31)]
    # icclim.indice(
    #     indice_name=eca_indices.Indice.TX90P.indice_name,
    #     in_files=files,
    #     # time_range=tr,
    #     base_period_time_range=bp,
    #     var_name="tasmax",
    #     slice_mode="MONTH",
    #     # transfer_limit_Mbytes=400,
    #     out_file=out_f,
    #     # out_unit="pouet"
    #     # threshold=[20, 30]
    #     # save_percentile=True,
    # )


    # CLIMPACT
    client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    bp = [datetime.datetime(1991, 1, 1), datetime.datetime(1999, 12, 31)]
    tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2010, 12, 31)]
    out_f = "netcdf_files/output/yolo.nc"
    icclim.indice(
        indice_name="r99p",
        in_files="netcdf_files/climpact.sampledata.gridded.1991-2010.nc",
        # var_name="tmax",
        slice_mode="JJA",
        base_period_time_range=bp,
        time_range=tr,
        # out_unit="%",
        transfer_limit_Mbytes=200,
        out_file=out_f,
    )

    # icclim.icclim.indice(
    #     in_files=files,
    #     var_name="tmax",
    #     user_indice={
    #         "indice_name": "swag_life",
    #         "calc_operation": "nb_events",
    #         "logical_operation": "gt",
    #         "thresh": 0,
    #         "date_event": True,
    #     },
    #     # out_unit="days",
    #     slice_mode="month",
    #     out_file=out_f,
    #     # transfer_limit_Mbytes=200
    # )

    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")


if __name__ == "__main__":
    run()

# COORDS = dict(
#         time=pd.date_range("2042-01-01", periods=200,
#                            freq=pd.DateOffset(days=1)),
#     )
#     da = xr.DataArray(
#         np.random.rand(200 * 3500 * 350).reshape((200, 3500, 350)),
#         dims=('time', 'x', 'y'),
#         coords=COORDS
#     ).chunk(dict(time=-1, x=100, y=100))
#     resampled = da.resample(time="MS")
#
#     for label, sample in resampled:
#         # sample = sample.compute()
#         idx = sample.argmax('time')
#         sample.isel(time=idx)
