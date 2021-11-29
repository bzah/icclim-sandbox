import xclim.indicators.seaIce
import xclim.indicators.atmos
import xclim.indicators.land

clix_indices = [("fd", "number_of_days_with_air_temperature_below_threshold"),
                ("tnlt2",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tnltm2",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tnltm20",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("id", "number_of_days_with_air_temperature_below_threshold"),
                ("su", "number_of_days_with_air_temperature_above_threshold"),
                ("txge30",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("txge35",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tr", "number_of_days_with_air_temperature_above_threshold"),
                ("tmge5",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tmlt5",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tmge10",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tmlt10",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tngt(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tnlt(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tnge(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tnle(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("txgt(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("txlt(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("txge(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("txle(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tmgt(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tmlt(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("tmge(TT)",
                 "number_of_days_with_air_temperature_above_threshold"),
                ("tmle(TT)",
                 "number_of_days_with_air_temperature_below_threshold"),
                ("ctngt(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("cfd",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("csu",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctnlt(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("ctnge(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctnle(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("ctxgt(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctxlt(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("ctxge(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctxle(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("ctmgt(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctmlt(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("ctmge(TT)",
                 "spell_length_of_days_with_air_temperature_above_threshold"),
                ("ctmle(TT)",
                 "spell_length_of_days_with_air_temperature_below_threshold"),
                ("txx", "air_temperature"),
                ("tnx", "air_temperature"),
                ("txn", "air_temperature"),
                ("tnn", "air_temperature"),
                ("txm", "air_temperature"),
                ("tnm", "air_temperature"),
                ("tmx", "air_temperature"),
                ("tmn", "air_temperature"),
                ("tmm", "air_temperature"),
                ("txmax", "air_temperature"),
                ("tnmax", "air_temperature"),
                ("txmin", "air_temperature"),
                ("tnmin", "air_temperature"),
                ("txmean", "air_temperature"),
                ("tnmean", "air_temperature"),
                ("tmmax", "air_temperature"),
                ("tmmin", "air_temperature"),
                ("tmmean", "air_temperature"),
                ("wsdi", "no_standard_name"),
                ("wsdi(ND)", "no_standard_name"),
                ("csdi", "no_standard_name"),
                ("csdi(ND)", "no_standard_name"),
                ("tn10pNOT", "no_standard_name"),
                ("tx10pNOT", "no_standard_name"),
                ("tn90pNOT", "no_standard_name"),
                ("tx90pNOT", "no_standard_name"),
                ("tg10pNOT", "no_standard_name"),
                ("tg90pNOT", "no_standard_name"),
                ("txgt50pNOT", "no_standard_name"),
                ("txgt(PRC)pNOT", "no_standard_name"),
                ("tngt(PRC)pNOT", "no_standard_name"),
                ("tmgt(PRC)pNOT", "no_standard_name"),
                ("txlt(PRC)pNOT", "no_standard_name"),
                ("tnlt(PRC)pNOT", "no_standard_name"),
                ("tmlt(PRC)pNOT", "no_standard_name"),
                ("dtrNOT", "no_standard_name"),
                ("vdtrNOT", "no_standard_name"),
                ("etrNOT", "no_standard_name"),
                ("tx95t", "air_temperature"),
                ("tx(PRC)pctlNOT", "air_temperature"),
                ("tn(PRC)pctlNOT", "air_temperature"),
                ("tm(PRC)pctlNOT", "air_temperature"),
                ("tx(D)tn(D)", "no_standard_name"),
                ("tx(D)tn(D)gt(PRC)p", "no_standard_name"),
                ("txb(D)tnb(D)", "no_standard_name"),
                ("tx(D)tn[D)lt(PRC)p", "no_standard_name"),
                ("hd17", "integral_wrt_time_of_air_temperature_excess"),
                (
                    "hddheat(TT)",
                    "integral_wrt_time_of_air_temperature_deficit"),
                ("ddgt(TT)", "integral_wrt_time_of_air_temperature_excess"),
                ("cddcold(TT)", "integral_wrt_time_of_air_temperature_excess"),
                ("ddlt(TT)", "integral_wrt_time_of_air_temperature_deficit"),
                ("gddgrow(TT)", "integral_wrt_time_of_air_temperature_excess"),
                ("gd4", "integral_wrt_time_of_air_temperature_excess"),
                ("gsl", "no_standard_name"),
                ("gsstart", "no_standard_name"),
                ("gsend", "no_standard_name"),
                ("gsgdd", "integral_wrt_time_of_air_temperature_excess"),
                ("HI", "no_standard_name"),
                ("BEDD", "no_standard_name"),
                ("HWN(EHF/Tx90/Tn90)", "no_standard_name"),
                ("HWF(EHF/Tx90/Tn90)", "no_standard_name"),
                ("HWD(EHF/Tx90/Tn90)", "no_standard_name"),
                ("HWM(EHF/Tx90/Tn90)", "no_standard_name"),
                ("HWA(EHF/Tx90/Tn90)", "no_standard_name"),
                ("CWN_ECF", "no_standard_name"),
                ("CWF_ECF", "no_standard_name"),
                ("CWD_ECF", "no_standard_name"),
                ("CWM_ECF", "no_standard_name"),
                ("CWA_ECF", "no_standard_name"),
                ("UTCI", "no_standard_name"),
                ("TCI", "no_standard_name"),
                ("TCI60", "no_standard_name"),
                ("TCI80", "no_standard_name"),
                ("r10mm",
                 "number_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("r20mm",
                 "number_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("r(RT)mm",
                 "number_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("wetdays",
                 "number_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("rr1",
                 "number_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("cdd",
                 "spell_length_of_days_with_lwe_thickness_of_precipitation_amount_below_threshold"),
                ("cwd",
                 "spell_length_of_days_with_lwe_thickness_of_precipitation_amount_above_threshold"),
                ("prcptot", "lwe_thickness_of_precipitation_amount"),
                ("sdii", "lwe_precipitation_rate"),
                ("r95ptot", "no_standard_name"),
                ("r99ptot", "no_standard_name"),
                ("r(PRC)ptot", "no_standard_name"),
                ("r95p", "no_standard_name"),
                ("r99p", "no_standard_name"),
                ("r(PRC)pctlNOT", "lwe_thickness_of_precipitation_amount"),
                ("rx1day", "lwe_thickness_of_precipitation_amount"),
                ("rx5day", "lwe_thickness_of_precipitation_amount"),
                ("rx(ND)day", "lwe_thickness_of_precipitation_amount"),
                ("r75ptot", "no_standard_name"),
                ("r95p", "no_standard_name"),
                ("SPI", "no_standard_name"),
                ("SPEI", "no_standard_name"),
                ("SPI6", "no_standard_name"),
                ("SPI3", "no_standard_name"),
                ("PET", "no_standard_name"),
                ("rh", "relative_humidity"),
                ("rr", "lwe_thickness_of_precipitation_amount"),
                ("pp", "air_pressure_at_sea_level "),
                ("tg", "air_temperature"),
                ("tn", "air_temperature"),
                ("tx", "air_temperature"),
                ("sd", "surface_snow_thickness"),
                ("sd1",
                 "number_of_days_with_surface_snow_thickness_above_threshold"),
                ("sd5cm",
                 "number_of_days_with_surface_snow_thickness_above_threshold"),
                ("sd50cm",
                 "number_of_days_with_surface_snow_thickness_above_threshold"),
                ("sd(D)cm",
                 "number_of_days_with_surface_snow_thickness_above_threshold"),
                ("ss", "duration_of_sunshine"),
                ("SSp", "no_standard_name"),
                ("fxx", "wind_speed_of_gust"),
                ("fg6bft", "number_of_days_with_wind_speed_above_threshold"),
                ("fgcalm", "number_of_days_with_wind_speed_below_threshold"),
                ("fg", "wind_speed"),
                ("CC", "no_standard_name"),
                ("CC6", "no_standard_name"),
                ("CD", "no_standard_name"),
                ("CW", "no_standard_name"),
                ("WD", "no_standard_name"),
                ("WW", "no_standard_name"),
                ("nzeroNOT ", "no_standard_name"),
                ("faf ", "no_standard_name"),
                ("lsf ", "no_standard_name"),
                ("maxdtrNOT", "no_standard_name"),
                ("txmax5daymean", "air_temperature"), ]


def get_xclim_index_name(module):
    acc = []
    for a in list(filter(lambda x: x[0] != "_", dir(module))):
        acc.append(
            (a,
             module.__dict__[a]
             .cf_attrs[0]
             .get("standard_name", "no_standard_name")))
    return acc


def main():
    xclim_indices = []
    xclim_indices += get_xclim_index_name(xclim.indicators.seaIce)
    xclim_indices += get_xclim_index_name(xclim.indicators.atmos)
    xclim_indices += get_xclim_index_name(xclim.indicators.land)
    # print(f"XCLIM indices: {xclim_indices}")

    in_xclim_not_in_clix = []
    in_clix_not_in_xclim = []
    in_both = []
    for xc in xclim_indices:
        if xc[1] != "no_standard_name":
            if xc[1].upper() not in list(
                    map(lambda clix: clix[1].upper(), clix_indices)):
                in_xclim_not_in_clix.append(xc)
            else:
                in_both.append((xc, "by standard_name"))
        else:
            if xc[0].upper() not in list(
                    map(lambda clix: clix[0].upper(), clix_indices)):
                in_xclim_not_in_clix.append(xc)
            else:
                in_both.append((xc, "by short name"))

        # if xc.upper() not in list(
        #         map(lambda clix: clix[1].upper, clix_indices)):
        #     in_xclim_not_in_clix.append(xc)
        # else:
        #     in_both.append(xc)
    for clix in clix_indices:
        if clix[1] != "no_standard_name":
            if clix[1].upper() not in list(
                    map(lambda ind: ind[1].upper(), xclim_indices)):
                in_clix_not_in_xclim.append(clix)
        else:
            if clix[0].upper() not in list(
                    map(lambda ind: ind[0].upper(), xclim_indices)):
                in_clix_not_in_xclim.append(clix)
        # if clix[1].upper() not in list(map(str.upper, xclim_indices)):
        #     in_clix_not_in_xclim.append(clix)

    print(f"in_xclim_not_in_clix indices: {in_xclim_not_in_clix}")
    print(f"in_clix_not_in_xclim indices: {in_clix_not_in_xclim}")
    print(f"in_both indices: {in_both}")


if __name__ == '__main__':
    main()
