"""
Sandbox for icclim v5 development.

It provides a few example of indices calculation with a few different files.
Beware, this is was not meant to be used by final user, use it at your own risk.
"""

import datetime
import glob
import time

import numpy as np
import pandas as pd

import icclim.ecad_functions
import icclim
import xarray as xr
from distributed import Client
from icclim.icclim_logger import Verbosity


def run():
    time_start = time.perf_counter()

    # AURELHY
    # out_f = "netcdf_files/output/tx90p_tx_montpe.nc"
    # # client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    # files = "/Users/aoun/workspace/icclim/netcdf_files/yolo_in2.nc"
    # bp = [datetime.datetime(2000, 1, 1), datetime.datetime(2001, 12, 31)]
    # # tr = [datetime.datetime(2000, 1, 1), datetime.datetime(2002, 12, 31)]
    # icclim.indice(
    #     index_name=eca_indices.Indice.TX90P.index_name,
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

    # #CLIMPACT
    # climp_file = "netcdf_files/climpact.sampledata.gridded.1991-2010.nc"
    # client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    # bp = [datetime.datetime(1991, 1, 1), datetime.datetime(1999, 12, 31)]
    # tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2010, 12, 31)]
    # icclim.indice(
    #     index_name="tx90p",
    #     in_files="netcdf_files/climpact.sampledata.gridded.1991-2010.nc",
    #     # var_name="tmax",
    #     slice_mode="DJF",
    #     base_period_time_range=bp,
    #     time_range=tr,
    #     out_unit="%",
    #     transfer_limit_Mbytes=200,
    #     out_file="netcdf_files/output/yolo.nc",
    # )

    # BIG PR file
    # big_pr_file = "netcdf_files/pr_day_CanESM5_ssp585_r10i1p1f1_gn_20150101-21001231.nc"
    # client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    # bp = [datetime.datetime(2015, 1, 1), datetime.datetime(2020, 12, 31)]
    # tr = [datetime.datetime(2022, 1, 1), datetime.datetime(2030, 12, 31)]
    # out_f = "netcdf_files/output/yolo.nc"
    # icclim.indice(
    #         index_name="r99p",
    #         in_files=big_pr_file,
    #         # var_name="tmax",
    #         slice_mode="JJA",
    #         base_period_time_range=bp,
    #         time_range=tr,
    #         # out_unit="%",
    #         transfer_limit_Mbytes=200,
    #         out_file=out_f,
    # )

    # BIG TAS file
    big_tas_file = "netcdf_files/tas_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20650101-21001231.nc"
    big_tas_file = "/Users/aoun/workspace/icclim/netcdf_files/ta_day_CanESM5_ssp585_r1i1p1f1_gn_20610101-20701231.nc"
    client = Client(memory_limit='14GB', timeout=20, n_workers=1, threads_per_worker=10)
    bp = [datetime.datetime(2060, 1, 1), datetime.datetime(2099, 12, 31)]
    tr = [datetime.datetime(2060, 1, 1), datetime.datetime(2061, 12, 31)]
    a = icclim.indice(indice_name='Su',
                     in_files=big_tas_file,
                     var_name='ta',
                     slice_mode='YS',
                     transfer_limit_Mbytes=200,
                     # out_unit="%",
                     time_range=tr,
                     base_period_time_range=bp,
                     out_file="netcdf_files/output/yolo3.nc",
                     logs_verbosity="low")

    icclim.indice(indice_name='SU',
                  in_files="path/to/tasmax.nc",
                  transfer_limit_Mbytes=200,
                  out_file="netcdf_files/output/my_glorious_output.nc",
                  logs_verbosity="high")


    # USER INDICES
    # icclim.icclim.indice(
    #     in_files=files,
    #     var_name="tmax",
    #     user_indice={
    #         "index_name": "swag_life",
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
