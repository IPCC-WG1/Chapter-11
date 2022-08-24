# IPCC-WG AR6 - Chapter 11 Figures

This repository contains data analysis scripts and visualization of many figures of
Chapter 11 of the Sixth Assessment Report (AR6) of the Intergovernmental Panel on
Climate Change (IPCC). It also includes parts of two figures in the Summary for Policymakers
(SPM).

## Figures

The following table shows which figures/ panels in the _SPM_, _Chapter 11_ and _Chapter 12_ in the IPCC AR6 report were created from this repository. It also shows which part (data analysis and visualization) is actually done in the repository.

| Figure    | Panel  | A<sup>*</sup> | V<sup>&dagger;</sup> | Notebook | Data Source |
| --------- | ------ |:--------:|:-------------:| -------- | ----------- |
| **Figure SPM.5**       | d   | x | x | [Figure_SPM.5_SM_map.ipynb](code/Figure_SPM.5_SM_map.ipynb) | CMIP6 |
| **Figure SPM.6**       | c   | x |   | [SMDroughtIndex.ipynb](code/SMDroughtIndex.ipynb) | CMIP6 |
| **Figure 11.2**        | -   | x | x | [Figure_11.2_obs_ts_plots.ipynb](code/Figure_11.2_obs_ts_plots.ipynb) | Dunn et al. (2020), Section 2.3.1.1.3 |
| **Figure 11.3**        | all | x | x | [Figure_11.3_TXx_scaling.ipynb](code/Figure_11.3_TXx_scaling.ipynb) | CMIP6 |
| **Figure 11.9**        | all | x | x | [Figure_11.9_HadEX3_maps.ipynb](code/Figure_11.9_HadEX3_maps.ipynb) | Dunn et al. (2020) |
| **Figure 11.10**       | all |   | x | [Figure_11.10_Wehner_temperature_bias.ipynb](code/Figure_11.10_Wehner_temperature_bias.ipynb) | Wehner et al. (2020) |
| **Figure 11.11**       | a-c | x | x | [Figure_11.11_TXx_map.ipynb](code/Figure_11.11_TXx_map.ipynb) | CMIP6 |
| **Figure 11.11**       | d-f | x | x | [Figure_11.11_TNn_map.ipynb](code/Figure_11.11_TNn_map.ipynb) | CMIP6 |
| **Figure 11.12**       | -   |   | x | [Figure_11.12_TXx_intensity_Li_et_al.ipynb](code/Figure_11.12_TXx_intensity_Li_et_al.ipynb) | Li et al. (2020) |
| **Figure 11.13**       | b-c |   | x | [Figure_11.13_Rx1day_trend_maps_Sun.ipynb](code/Figure_11.13_Rx1day_trend_maps_Sun.ipynb) | Sun et al. (2020) |
| **Figure 11.14**       | all |   | x | [Figure_11.14_Wehner_precipitation_bias.ipynb](code/Figure_11.14_Wehner_precipitation_bias.ipynb) | Wehner et al. (2020) |
| **Figure 11.15**       | -   |   | x | [Figure_11.15_Rx1day_intensity_Li_et_al.ipynb](code/Figure_11.15_Rx1day_intensity_Li_et_al.ipynb) | Li et al. (2020) |
| **Figure 11.16**       | all | x | x | [Figure_11.16_Rx1day_map.ipynb](code/Figure_11.16_Rx1day_map.ipynb) | CMIP6 |
| **Figure 11.17**       | all |   | x | [Figure_11.17_CDD_SPI_SPEI.ipynb](code/Figure_11.17_CDD_SPI_SPEI.ipynb) | Spinoni et al. (2019) |
| **Figure 11.18**       | all | x | x | [SMDroughtIndex.ipynb](code/SMDroughtIndex.ipynb) | CMIP6 |
| **Figure 11.19**       | a-c | x | x | [Figure_11.19_CDD_map.ipynb](code/Figure_11.19_CDD_map.ipynb) | CMIP6 |
| **Figure 11.19**       | d-f | x | x | [Figure_11.19_SM_map.ipynb](code/Figure_11.19_SM_map.ipynb) | CMIP6 |
| **Figure 11.19**       | g-l | x | x | [SMDroughtIndex.ipynb](code/SMDroughtIndex.ipynb) | CMIP6 |
| **Box 11.1, Figure 1** | all |   | x | [Box_11.1_Figure_1_Pfahl_2017.ipynb](code/Box_11.1_Figure_1_Pfahl_2017.ipynb) | Pfahl et al. (2017) |
| **Box 11.4, Figure 1** | -   |   | x | [Box_11.4_Figure_1_Sippel_2015.ipynb](code/Box_11.4_Figure_1_Sippel_2015.ipynb) | Sippel et al. (2015) |
| **Box 11.4, Figure 2** | -   | x | x | [Box_11.4_Figure_2_2018.ipynb](code/Box_11.4_Figure_2_2018.ipynb) | Hersbach et al. (2020) |
| **FAQ 11.1, Figure 1** | all | x | x | [FAQ_11.1_Figure_1_mean_vs_extreme.ipynb](code/FAQ_11.1_Figure_1_mean_vs_extreme.ipynb) | CMIP6 |
| **Figure 11.SM.1**     | all | x | x | [Figure_11.SM.1_TNn_scaling.ipynb](code/Figure_11.SM.1_TNn_scaling.ipynb) | CMIP6 |
| **Figure 12.4**        | j-l | x |   | [Figure_12.4_S12.4_SM_DataTable_Jerome.ipynb](code/Figure_12.4_S12.4_SM_DataTable_Jerome.ipynb) | CMIP6 |

<sup>*</sup>Analysis
<sup>&dagger;</sup> Visualisation

## Tables

The following shows which tables in Chapter 11 were created from this repository.

| Table    | Notebook | Data Source |
| -------- | -------- | ----------- |
| Table 11.SM.1   | [Table_11.SM.1_GSAT_anom.ipynb](code/Table_11.SM.1_GSAT_anom.ipynb) | CMIP5 & CMIP6 |
| Table 11.SM.2-8 | [Table_11.SM_cmip_indices_regional.ipynb](code/Table_11.SM_cmip_indices_regional.ipynb) | CMIP6 |


## Associated data repositories

There are three external data repositories associated with Chapter 11, which provide additional data and data used in the chapter in a computer accessible form.

### Global mean temperature anomalies for CMIP5 and CMIP6

The [cmip_temperatures](https://github.com/mathause/cmip_temperatures) repository provides mean temperature anomalies for different time periods for CMIP5 and CMIP6. It contains the same info as Table 11.SM.1. Note that these temperature anomalies are unassessed.

### Global warming levels for CMIP5 and CMIP6

The [cmip_warming_levels](https://github.com/mathause/cmip_warming_levels) repository documents the year when a certain global warming level was reached in CMIP5 and CMIP6 data. There is no associated table in the IPCC report.

### Multi-model-median regional means at warming levels for selected indices (CMIP6)

The [cmip_indices_regional](https://github.com/mathause/cmip_indices_regional) repository documents multi-model-median regional means at warming levels for selected indices (CMIP6) & contains the same info as Table 11.SM.2 through Table 11.SM.8.

## Regional Factsheets

The map insets for IPCC AR6 WGI [regional fact sheets](https://www.ipcc.ch/report/ar6/wg1/resources/factsheets/) were created in the [RegionalFactSheetsMaps.ipynb](code/RegionalFactSheetsMaps.ipynb) notebook. These maps were used for the following fact sheets: Introduction, Africa, Asia, Australasia, Central and South America, Europe, North and Central America, and Ocean.

## Data

This repository is provided _without_ data. The data volume (approximately 1 TB) prohibits an efficient distribution. In addition not all data is in the public domain.

There are several ways to obtain some or all the data
- The data can be provided upon reasonable request (either the whole data folder or parts).
- Some of the final data (i.e. exactly what is shown in the figure) is available from [CEDA](https://catalogue.ceda.ac.uk), (e.g. the data for [Figure SPM 5](https://catalogue.ceda.ac.uk/uuid/2787230b963942009e452255a3880609)).
- Some datasets are publicly available, e.g. [HadEX3](https://www.metoffice.gov.uk/hadobs/hadex3/).


## License

Copyright (c) 2021 ETH Zurich, Mathias Hauser.

This is free software; you can redistribute it and/or modify it under the terms of the
GNU General Public License as published by the Free Software Foundation, version 3  or
(at your option) any later version.

The code is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this code. If
not, see https://www.gnu.org/licenses/.


## Acknowledgment

We acknowledge the World Climate Research Programme (WCRP)'s Working Group on Coupled Modelling, which is responsible for CMIP and coordinated CMIP5 and CMIP6. We particularly thank the climate modeling groups for producing and making available their model output. We thank Urs Beyerle for downloading, archiving, and curating the CMIP5 and CMIP6 data at ETH Zurich.

## References


- Dunn, R. J. H., Alexander, L. V., Donat, M. G., Zhang, X., Bador, M., Herold, N., et al. (2020). Development of an Updated Global Land In Situ-Based Data Set of Temperature and Precipitation Extremes: HadEX3. J. Geophys. Res. Atmos. 125. doi:10.1029/2019JD032263.
- Hersbach, H., Bell, B., Berrisford, P., Hirahara, S., Horányi, A., Muñoz‐Sabater, J., et al. (2020). The ERA5 global reanalysis. Q. J. R. Meteorol. Soc. 146, 1999–2049. doi:10.1002/qj.3803.
- Li, C., Zwiers, F., Zhang, X., Li, G., Sun, Y., and Wehner, M. (2020a). Changes in annual extremes of daily temperature and precipitation in CMIP6 models. J. Clim. (submitted, 1–61. doi:10.1175/JCLI-D-19-1013.1.
- Pfahl, S., O’Gorman, P. A., and Fischer, E. M. (2017). Understanding the regional pattern of projected future changes in extreme precipitation. Nat. Clim. Chang. 7, 423. doi:10.1038/nclimate3287.
- Sippel, S., Zscheischler, J., Heimann, M., Otto, F. E. L., Peters, J., and Mahecha, M. D. (2015). Quantifying changes in
climate variability and extremes: Pitfalls and their overcoming. Geophys. Res. Lett. 42, 9990–9998.
doi:10.1002/2015GL066307.
- Spinoni, J., Barbosa, P., De Jager, A., McCormick, N., Naumann, G., Vogt, J. V., et al. (2019). A new global database of meteorological drought events from 1951 to 2016. J. Hydrol. Reg. Stud. 22, 100593.
doi:10.1016/J.EJRH.2019.100593.
- Sun, Q., Zhang, X., Zwiers, F., Westra, S., and Alexander, L. V (2020). A global, continental and regional analysis of changes in extreme precipitation. J. Clim., 1–52. doi:10.1175/JCLI-D-19-0892.1.
- Wehner, M., Gleckler, P., and Lee, J. (2020). Characterization of long period return values of extreme daily temperature and precipitation in the CMIP6 models: Part 1, model evaluation. Weather Clim. Extrem. 30, 100283. doi:10.1016/j.wace.2020.100283.
