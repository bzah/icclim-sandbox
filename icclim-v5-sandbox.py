import datetime
import glob
import time

import dask
import numpy as np
import pandas as pd
import xarray
import xclim
from xclim.core.calendar import percentile_doy

import ecad_functions
import icclim
import xarray as xr
from distributed import Client
from icclim.icclim_logger import Verbosity

from models.ecad_indices import EcadIndex

def aurelhy():
    # AURELHY from CNRM
    out_f = "netcdf_files/output/tx90p_tx_montpe.nc"
    # client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    files = "/Users/aoun/workspace/icclim/netcdf_files/yolo_in2.nc"
    bp = [datetime.datetime(2000, 1, 1), datetime.datetime(2001, 12, 31)]
    # tr = [datetime.datetime(2000, 1, 1), datetime.datetime(2002, 12, 31)]
    icclim.indice(
        index_name=EcadIndex.TX90P.index_name,
        in_files=files,
        # time_range=tr,
        base_period_time_range=bp,
        var_name="tasmax",
        slice_mode="MONTH",
        # transfer_limit_Mbytes=400,
        out_file=out_f,
        # out_unit="pouet"
        # threshold=[20, 30]
        # save_percentile=True,
    )

def climpact():
    # CLIMPACT
    climp_file = "netcdf_files/climpact.sampledata.gridded.1991-2010.nc"
    client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    bp = [datetime.datetime(1991, 1, 1), datetime.datetime(1999, 12, 31)]
    tr = [datetime.datetime(1991, 1, 1), datetime.datetime(2010, 12, 31)]
    icclim.indice(
        index_name="tx90p",
        in_files="netcdf_files/climpact.sampledata.gridded.1991-2010.nc",
        # var_name="tmax",
        slice_mode="DJF",
        base_period_time_range=bp,
        time_range=tr,
        out_unit="%",
        transfer_limit_Mbytes=200,
        out_file="netcdf_files/output/yolo.nc",
    )
    # USER INDICES
    icclim.index(
        in_files="./netcdf_files/climpact.sampledata.gridded.1991-2010.nc",
        var_name="tmax",
        user_index={
            "index_name":        "swag_life",
            "calc_operation":    "nb_events",
            "logical_operation": "gt",
            "thresh":            0,
            "date_event":        True,
        },
        # out_unit="days",
        slice_mode="month",
        out_file="./netcdf_files/output/yolo.nc",
        # transfer_limit_Mbytes=200
    )
    # client = Client(memory_limit='10GB', n_workers=1, threads_per_worker=8)
    # ds = xarray.open_dataset("./netcdf_files/climpact.full.nc")
    # tasmax_da = ds.tmax.chunk({"time": "auto", "lat": 20, "lon": 40})
    # above_thresh_da = tasmax_da > 25
    # result = above_thresh_da.resample("MS").sum(dim="time")
    # result.compute()
    #
    # client = Client(memory_limit='10GB', n_workers=1, threads_per_worker=8)
    # icclim.index(in_files="./netcdf_files/climpact.full.nc",
    #              user_index={'index_name':        'reversed_su',
    #                          'calc_operation':    'nb_events',
    #                          'logical_operation': '<=',
    #                          'thresh':            25},
    #              var_name="tmax",
    #              slice_mode='month',
    #              out_file="reversed_su.nc")

def big_pr_example():
    # BIG PR file
    big_pr_file = "netcdf_files/pr_day_CanESM5_ssp585_r10i1p1f1_gn_20150101-21001231.nc"
    client = Client(memory_limit='8GB', timeout=20, n_workers=1, threads_per_worker=8)
    bp = [datetime.datetime(2015, 1, 1), datetime.datetime(2020, 12, 31)]
    tr = [datetime.datetime(2022, 1, 1), datetime.datetime(2030, 12, 31)]
    out_f = "netcdf_files/output/yolo.nc"
    icclim.indice(
            index_name="r99p",
            in_files=big_pr_file,
            # var_name="tmax",
            slice_mode="JJA",
            base_period_time_range=bp,
            time_range=tr,
            # out_unit="%",
            transfer_limit_Mbytes=200,
            out_file=out_f,
    )

def big_tas_example():
    # BIG TAS file
    big_tas_file = "netcdf_files/tas_day_INM-CM5-0_ssp585_r1i1p1f1_gr1_20650101-21001231.nc"
    bp = [datetime.datetime(2065, 1, 1), datetime.datetime(2073, 12, 31)]
    # tr = [datetime.datetime(2060, 1, 1), datetime.datetime(2061, 12, 31)]

    # big_tas_file = "/Users/aoun/workspace/icclim/netcdf_files/ta_day_CanESM5_ssp585_r1i1p1f1_gn_20610101-20701231.nc"
    # bp = [datetime.datetime(2060, 1, 1), datetime.datetime(2070, 12, 31)]
    # tr = [datetime.datetime(2060, 1, 1), datetime.datetime(2061, 12, 31)]
    # client = Client(memory_limit='12GB', timeout=20, n_workers=1, threads_per_worker=10)
    tg90p = icclim.index(index_name='TG90p',
                     in_files=big_tas_file,
                     # var_name='tas',
                     slice_mode='YS',
                     transfer_limit_Mbytes=200,
                     # out_unit="%",
                     # time_range=tr,
                     base_period_time_range=bp,
                     out_file="netcdf_files/output/yolo3.nc",
                     logs_verbosity="low")
    su = icclim.indice(indice_name='SU',
                  in_files="path/to/tasmax.nc",
                  transfer_limit_Mbytes=200,
                  out_file="netcdf_files/output/my_glorious_output.nc",
                  logs_verbosity="high")

def knmi_pr():
    # KNMI PR
    rr_files = glob.glob(f"netcdf_files/knmi/clean/*rr*.nc")
    a = icclim.index(index_name='rx5day',
                     in_files=rr_files,
                     slice_mode='YS',
                     out_file="netcdf_files/output/knmi_result_rx5day.nc")

def knmi_tg():
    # KNMI TG
    tg_files = glob.glob(f"netcdf_files/knmi/clean/*tg*.nc")
    a = icclim.index(index_name='gd4',
                     in_files=tg_files,
                     slice_mode='YS',
                     out_file="netcdf_files/output/knmi_result_gd4.nc")

def knmi_tx():
    # KNMI TX
    tx_files = glob.glob(f"netcdf_files/knmi/clean/*tx*.nc")
    bp = [datetime.datetime(1901, 1, 1), datetime.datetime(1921, 12, 31)]
    icclim.index(index_name='tx90p',
                 in_files=tx_files,
                 base_period_time_range=bp,
                 slice_mode='YS',
                 out_file="netcdf_files/output/out.nc")

def knmi_tn():
    # KNMI TN
    tn_files = glob.glob(f"netcdf_files/knmi/clean/*tn*.nc")
    a = icclim.index(index_name='fd',
                     in_files=tn_files,
                     slice_mode='YS',
                     out_file="netcdf_files/output/knmi_result_fd.nc",)

def huge_tas_files():
    # Christian tasmax (CMIP6)
    # -- No bootstrap
    # client = Client(address="tcp://127.0.0.1:63450")
    # client = Client(memory_limit='16GB', n_workers=1, threads_per_worker=4)
    # dask.config.set({"array.slicing.split_large_chunks": False})
    # dask.config.set({"array.chunk-size": "20 MB"})
    # print(client)
    # # studied period
    # dt1 = datetime.datetime(2081, 1, 1)
    # dt2 = datetime.datetime(2100, 12, 31)
    # # reference period
    # dt1r = datetime.datetime(1981, 1, 1)
    # dt2r = datetime.datetime(2000, 12, 31)
    # out_f = 'tx90p_icclim.nc'
    # filenames = glob.glob('../icclim/netcdf_files/page/tasmax_day*.nc')
    # icclim.index(index_name='TX90p',
    #              in_files=filenames,
    #              var_name='tasmax',
    #              base_period_time_range=[dt1r, dt2r],
    #              time_range=[dt1, dt2],
    #              out_file=out_f,
    #              logs_verbosity='HIGH',
    #              slice_mode='JJA',
    #              out_unit='%',
    #              )
    # -- ~Same but with pure xclim
    # tr = slice("2081-01-01", "2100-12-31")
    # bp = slice("1981-01-01", "2000-12-31")
    # ds = xr.open_mfdataset(filenames, chunks="auto", parallel=True)
    # da = ds.tasmax
    # da_per = percentile_doy(da.sel(time=bp), 5, 90, alpha=1 / 3, beta=1 / 3,)
    # xclim.atmos.tx90p(da.sel(time=tr), da_per, freq="MS", bootstrap=False).compute()
    # -- With Bootstrap
    client = Client(memory_limit='16GB', n_workers=1, threads_per_worker=2)
    dask.config.set({"array.slicing.split_large_chunks": False})
    dask.config.set({"array.chunk-size": "200 MB"})
    dask.config.set({"distributed.worker.memory.target": "0.8"})
    dask.config.set({"distributed.worker.memory.spill": "0.9"})
    dask.config.set({"distributed.worker.memory.pause": "0.95"})
    dask.config.set({"distributed.worker.memory.terminate": "0.98"})
    dt1 = datetime.datetime(1981, 1, 1)
    dt2 = datetime.datetime(1992, 12, 31)
    bp = datetime.datetime(1981, 1, 1)
    bp2 = datetime.datetime(1989, 12, 31)
    out_f = 'tx90p_icclim.nc'
    filenames = glob.glob('./netcdf_files/page/tasmax_day*.nc')
    icclim.index(index_name='TX90p', in_files=filenames, var_name='tasmax',
                 slice_mode='JJA', base_period_time_range=[bp, bp2],
                 time_range=[dt1, dt2], out_unit='%', out_file=out_f,
                 logs_verbosity='LOW')

def bcc_csm2():
    # BCC-CSM2-MR
    tas_file = glob.glob(
        f"netcdf_files/tas_day_BCC-CSM2-MR_ssp585_r1i1p1f1_gn_20900101-21001231.nc")
    icclim.index(index_name='TG',
                 in_files=tas_file,
                 slice_mode='YS',
                 out_file="netcdf_files/output/yolo.nc")

def tg90p():
    time_start = time.perf_counter()

    client = Client(memory_limit='16GB', n_workers=1, threads_per_worker=8)
    dask.config.set({"array.chunk-size":                 f"{200} MB",
                     'array.slicing.split_large_chunks': False
                     })
    # huge_tas_file = "../icclim/netcdf_files/page/tasmax_day*.nc"
    # ds = xr.open_mfdataset(huge_tas_file, parallel=True)
    # big_tas_file = "../icclim/netcdf_files/tasmax_day_MIROC6_ssp585_r1i1p1f1_gn_20150101-20241231.nc"
    # ds = xr.open_mfdataset(big_tas_file, parallel=True)
    ds = xr.open_zarr("../xclim/xclim-zarr-store")
    da = ds.tasmax.chunk("auto")
    da_per = da.sel(time=slice("1981-01-01", "1991-12-31"))
    da = da.sel(time=slice("1981-01-01", "2000-12-31"))
    # da_per = da.sel(time=slice("1981-01-01", "1999-12-31"))
    # da = da.sel(time=slice("2015-01-01", "2024-12-31"))
    # da_per = da.sel(time=slice("2015-01-01", "2023-12-31"))
    tas_per = percentile_doy(da_per, window=5, per=90)
    a = xclim.atmos.tg90p(tas=da, t90=tas_per, freq="MS", bootstrap=True)

    a.compute()

    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")

if __name__ == "__main__":
    time_start = time.perf_counter()

    huge_tas_files()

    time_elapsed = time.perf_counter() - time_start
    print(time_elapsed, " secs")
