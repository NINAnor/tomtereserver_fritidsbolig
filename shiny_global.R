library(dplyr)

reserve_data <- list()
reserve_data[["planomrade"]] <- readRDS("data/reserve_omrader_centroid.rds")
reserve_data[["kommune"]] <- readRDS("data/reserve_kommuner.rds")
reserve_data[["fylke"]] <- readRDS("data/reserve_fylker.rds")

allzips <- reserve_data[["planomrade"]]
row.names(allzips) <- allzips$poid
row.names(reserve_data[["planomrade"]]) <- reserve_data[["planomrade"]]$poid
row.names(reserve_data[["kommune"]]) <- reserve_data[["kommune"]]$komunenummer
row.names(reserve_data[["fylke"]]) <- reserve_data[["fylke"]]$fylkenumer

cleantable <- allzips #%>%
#   select(
#     City = city.x,
#     State = state.x,
#     Zipcode = zipcode,
#     Rank = rank,
#     Score = centile,
#     Superzip = superzip,
#     Population = adultpop,
#     College = college,
#     Income = income,
#     Lat = latitude,
#     Long = longitude
#   )

# Choices for drop-downs
vars <- list()
vars[["planomrade"]] <- c("Tomtereserve i dekar" = "tomtereserve_daa",
                          "Plankategori" = "plankategori",
                             "Arealformål" = "arealformal",
                             "Arealbruksstatus" = "arealbruksstatus",
                             "Vedtak endelig plan (dato)" = "vedtakendeligplandato",
                             "Ikrafttredelsesdato" = "ikrafttredelsesdato",
                             "Plantype" = "plantype",
                             "Lovreferansetype" = "lovreferansetype",
                             "Utnyttingstall" = "utnyttingstall",
                             "Utnyttingstall (minimum)" = "utnyttingstall_minimum",
                             "Areal i m2" = "areal_m2",
                             "Antall hytter per dekar" = "antall_hytter_per_daa",
                             "Antall hytter på planområde" = "antall_bygninger",
                             "Areal utilgjengelig for bebyggelse i dekar" = "ubebyggbar_areal_daa",
                             #"Areal tilgjengelig for bebyggelse i dekar" = "bebyggbar_areal_daa",
                             "Støre sammenhengende areal tilgjengelig for bebyggelse i daa" = "tilgjengelig_stor_areal_daa",
                             #"Andel regulert areal i planområde" = "reguleringsplan_percent",
                             "Avstand til kystlinje i meter" = "avstand_kystkontur_m",
                             #"Hoved landskapstype i planområde" = "landskap",
                             "Overlapp med myr i dekar" = "myr_areal_daa",
                             "Areal over skoggrensen i dekar" = "fjell_areal_daa",
                             "Areal i kystsonen i dekar" = "kystsone_daa",
                             "Areal i kystsone 1 i dekar" = "kystsone_1_daa",
                             "Areal i kystsone 2 i dekar" = "kystsone_2_daa",
                             "Areal i kystsone 3 i dekar" = "kystsone_3_daa",
                             "Overlapp med verneområder i dekar" = "naturvern_daa",
                             "Overlapp med strenge verneområder i dekar" = "naturvern_streng_daa",
                             "Overlapp med foreslåtte verneområder i dekar" = "naturvern_foreslatt_daa",
                             "Overlapp med foreslåtte strenge verneområder i dekar" = "naturvern_foreslatt_streng_daa",
                             "Overlapp med kjente verdifulle naturtyper i dekar" = "naturtyper_daa",
                             "Overlapp med kjente naturtyper av høy verdi i dekar" = "naturtyper_hoy_verdi_daa",
                             "Overlapp funksjonsområder for reindrift i dekar" = "reindrift_funksjonsomrader_daa",
                             "Overlapp administrative områder for reindrift i dekar" = "reindrift_adminomrader_daa",
                             "Overlapp med villreinområder i dekar" = "villrein_daa",
                             "Overlapp med villreinområder for helårsbruk i dekar" = "villrein_helar_daa",
                             "Avstand til nærmeste vei i meter" = "avstand_vei_m",
                             "Kjøreavstand til nærmeste tettsted" = "kjoereavstand_naermeste_tettsted_minutes",
                             "Befolkning innen 2 timers kjøretid" = "befolkning_2h",
                             "Befolkning innen 3 timers kjøretid" = "befolkning_3h",
                             "Befolkning innen 4 timers kjøretid" = "befolkning_4h"
                             )
vars[["kommune"]] <- c(
  "Antall fritidsbolig" = "antall_fritidsbolig",
  "Plankategorier" = "plankategorier",
  "Antall fritidsbolig per daa planomrade" = "antall_hytter_per_daa_avg",
  "Areal utilgjengelig for bebyggelse i daa" = "ubebyggbar_areal_daa_sum",
  "Større arealer tilgjengelig for bebyggelse i daa" = "tilgjengelig_stor_areal_daa_sum",
  # "reguleringsplan_percent_avg",
  "Tomtereserve i daa" = "tomtereserve_daa_sum",
  "Regulert tomtereserve i daa" = "tomtereserve_regulert_daa_sum",
  "Gjennomsnittlig avstand til kystkontur i meter" = "avstand_kystkontur_m_avg",
  "Overlapp med myr i dekar" = "myr_areal_daa_sum",
  "Overlapp med myr som % av kommunens areal" = "myr_areal_perc",
  "Areal over skoggrensen i dekar" = "fjell_areal_daa_sum",
  "Areal over skoggrensen som % av kommunens areal" = "fjell_areal_perc",
  "Areal i kystsonen i dekar" = "kystsone_daa_sum",
  "Areal i kystsonen som % av kommunens areal" = "kystsone_perc",
  "Areal i kystsone 1 i dekar" = "kystsone_1_daa_sum",
  "Areal i kystsone 1 som % av kommunens areal" = "kystsone_1_perc",
  "Areal i kystsone 2 i dekar" = "kystsone_2_daa_sum",
  "Areal i kystsone 2 som % av kommunens areal" = "kystsone_2_perc",
  "Areal i kystsone 3 i dekar" = "kystsone_3_daa_sum",
  "Areal i kystsone 3 som % av kommunens areal" = "kystsone_3_perc",
  "Overlapp med verneområder i dekar" = "naturvern_daa_sum",
  "Overlapp med verneområder som % av kommunens areal" = "naturvern_perc",
  "Overlapp med strenge verneområder i dekar" = "naturvern_streng_daa_sum",
  "Overlapp med strenge verneområder som % av kommunens areal" = "naturvern_streng_perc",
  "Overlapp med foreslåtte verneområder i dekar" = "naturvern_foreslatt_daa_sum",
  "Overlapp med foreslåtte verneområder som % av kommunens areal" = "naturvern_foreslatt_perc",
  "Overlapp med foreslåtte strenge verneområder i dekar" = "naturvern_foreslatt_streng_daa_sum",
  "Overlapp med foreslåtte strenge verneområder som % av kommunens areal" = "naturvern_foreslatt_streng_perc",
  "Overlapp med kjente verdifulle naturtyper i dekar" = "naturtyper_daa_sum",
  "Overlapp med kjente verdifulle naturtyper som % av kommunens areal" = "naturtyper_perc",
  "Overlapp med kjene naturtyper av høy verdi i dekar" = "naturtyper_hoy_verdi_daa_sum",
  "Overlapp med kjene naturtyper av høy verdi som % av kommunens areal" = "naturtyper_hoy_verdi_perc",
  "Overlapp funksjonsområder for reindrift i dekar" = "reindrift_funksjonsomrader_daa_sum",
  "Overlapp funksjonsområder for reindrift som % av kommunens areal" = "reindrift_funksjonsomrader_perc",
  "Overlapp administrative områder for reindrift i dekar" = "reindrift_adminomrader_daa_sum",
  "Overlapp administrative områder for reindrift som % av kommunens areal" = "reindrift_adminomrader_perc",
  "Overlapp med villreinområder i dekar" = "villrein_daa_sum",
  "Overlapp med villreinområder som % av kommunens areal" = "villrein_perc",
  "Overlapp med villreinområder for helårsbruk i dekar" = "villrein_helar_daa_sum",
  "Overlapp med villreinområder for helårsbruk som % av kommunens areal" = "villrein_helar_perc",
  "Avstand til nærmeste vei i meter" = "avstand_vei_m_avg",
  "Kjøretid (i minutter) til nærmeste tettsted i snitt" = "kjoereavstand_naermeste_tettsted_minutes_avg",
  "Befolkning innen 2 timers kjøretid i snitt" = "befolkning_2h_avg",
  "Befolkning innen 3 timers kjøretid i snitt" = "befolkning_3h_avg",
  "Befolkning innen 4 timers kjøretid i snitt" = "befolkning_4h_avg")
vars[["fylke"]] <- vars[["kommune"]]


aggregation_level_name <- c("planomrade" = "Planområde",
                            "kommune" = "Kommune",
                            "fylke" = "Fylke")

categorical_variables <- c("fylkenummer", "plankategorier", "plankategori", "vedtakendeligplandato", "ikrafttredelsesdato", "plantype", "lovreferansetype", "landskap")
