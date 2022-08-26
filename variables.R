#Set connection parameters
pg_host <- "gisdata-db.nina.no"
pg_db <- 'gisdata'
pg_user <- Sys.getenv("LOGNAME")

workdir <- "/data/P-Prosjekter/15227000_kartlegging_av_tomtereserve_for_fritidsbebyggels/"
plot_dir <- file.path(workdir, "plots")

schema <- "tomtereserver_fritidsbolig"
#table <- "fritidsbygg_vs_plan_alle"
table_plans <- "plan_og_fritidsbygg"
table_reserve <- "fritidsboligformal"

# columns <- dbListFields(con, name=Id(schema=schema, table = table_reserve))
# columns <- columns[!columns %in% c("geom", "bygningcentroid")]

columns <- c("poid",
             "areal_m2",
             "lokalid",
             "planidentifikasjon",
             "plankategori",
             "arealformal",
             "arealbruksstatus",
             "vedtakendeligplandato",
             "ikrafttredelsesdato",
             "omradenavn",
             "beskrivelse",
             "plannavn",
             "plantype",
             "planstatus",
             "planbestemmelse",
             "lovreferansetype",
             "vertikalniva",
             "link",
             "utnyttingstype",
             "utnyttingstall",
             "utnyttingstall_minimum",
             "informasjon",
             "utm_33_x",
             "utm_33_y",
             "geom_centroid",
             "antall_hytter_per_m2",
             "antall_bygninger",
             "ubebyggbar_areal_m2",
             "bebygd_areal_m2",
             "tilgjengelig_stor_areal_m2",
             "reguleringsplan_percent",
             "bebygd_areal_m2",
             "landskap",
             "myr_areal_m2",
             "fjell_areal_m2",
             "kystsone_1_m2",
             "kystsone_2_m2",
             "kystsone_3_m2",
             "avstand_kystkontur_m",
             "naturvern_m2",
             "naturvern_streng_m2",
             "naturvern_foreslatt_m2",
             "naturvern_foreslatt_streng_m2",
             "naturtyper_m2",
             "naturtyper_hoy_verdi_m2",
             "reindrift_funksjonsomrader_m2",
             "reindrift_adminomrader_m2",
             "villrein_m2",
             "villrein_helar_m2",
             "kommune_landareal_km2",
             "kommunenummer_aktuell",
             "tilgjengelig_stor_areal_m2",
             "befolkning_2h",
             "kjoereavstand_avg_2h",
             "kjoereavstand_naermeste_tettsted_minutes",
             "befolkning_3h",
             "kjoereavstand_avg_3h",
             "befolkning_4h",
             "kjoereavstand_avg_4h",
             "kystsone_m2",
             "tomtereserve_m2",
             "tomtereserve_antall_boliger")

descriptiv_vars <- list("antall_hytter_per_m2" = "Antall hytter per m2",
                     "antall_bygninger" = "Antall hytter på planområde",
                     "ubebyggbar_areal_m2" = "Areal utilgjengelig for bebyggelse i m2",
                     "bebyggbar_areal_m2" = "Areal tilgjengelig for bebyggelse i m2",
                     "tilgjengelig_stor_areal_m2" = "Større sammenhengende areal tilgjengelig for bebyggelse i m2",
                     "reguleringsplan_percent" = "Andel regulert areal i planområde",
                     "avstand_kystkontur_m" = "Avstand til kystlinje i m",
                     "landskap" = "Hoved landskapstype i planområde")

conflict_vars <- list("myr_areal_daa" = "Overlapp med myr i dekar",
                   "fjell_areal_daa" = "Areal over skoggrensen i dekar",
                   "kystsone_daa" = "Areal i kystsonen i dekar",
                   "kystsone_1_daa" = "Areal i kystsone 1 i dekar",
                   "kystsone_2_daa" = "Areal i kystsone 2 i dekar",
                   "kystsone_3_daa" = "Areal i kystsone 3 i dekar",
                   "naturvern_daa" = "Overlapp med verneområder i dekar",
                   "naturvern_streng_daa" = "Overlapp med strenge verneområder i dekar",
                   "naturvern_foreslatt_daa" = "Overlapp med foreslåtte verneområder i dekar",
                   "naturvern_foreslatt_streng_daa" = "Overlapp med foreslåtte strenge verneområder i dekar",
                   "naturtyper_daa" = "Overlapp med kjene verdifulle naturtyper i dekar",
                   "naturtyper_hoy_verdi_daa" = "Overlapp med kjene naturtyper av høy verdi i dekar",
                   "reindrift_funksjonsomrader_daa" = "Overlapp med funksjonsområder for reindrift i dekar",
                   "reindrift_adminomrader_daa" = "Overlapp med administrative områder for reindrift i dekar",
                   "villrein_daa" = "Overlapp med villreinområder i dekar",
                   "villrein_helar_daa" = "Overlapp med villreinområder for helårsbruk i dekar")

use_vars <- list(
  "avstand_vei_m" = "Avstand til nærmeste vei i meter",
  "befolkning_2h" = "Befolkning innen 2 timers kjøretid",
  "kjoereavstand_naermeste_tettsted_minutes" = "Kjøreavstand til nærmeste tettsted",
  "befolkning_3h" = "Befolkning innen 3 timers kjøretid",
  "befolkning_4h" = "Befolkning innen 4 timers kjøretid"
  )

continous_vars <- list("tomtereserve_daa" = "Tomtereserve i dekar",
                       "tomtereserve_antall_boliger" = "Tomtereserve i estimert antall fritidsboliger som kan bygges",
                       "antall_hytter_per_daa" = "Antall hytter per dekar planområde",
                        "antall_bygninger" = "Antall hytter i planområde",
                        "ubebyggbar_areal_daa" = "Areal utilgjengelig for bebyggelse i dekar",
                        #"bebyggbar_areal_daa" = "Areal tilgjengelig for bebyggelse i dekar",
                        "tilgjengelig_stor_areal_daa" = "Støre sammenhengende areal tilgjengelig for bebyggelse i dekar",
                        #"reguleringsplan_percent" = "Andel regulert areal i planområde",
                        "avstand_kystkontur_m" = "Avstand til kystlinje i meter",
                        "avstand_vei_m" = "Avstand til nærmeste vei i meter",
                        "befolkning_2h" = "Befolkning innen 2 timers kjøretid",
                        "kjoereavstand_naermeste_tettsted_minutes" = "Kjøreavstand til nærmeste tettsted",
                        "befolkning_3h" = "Befolkning innen 3 timers kjøretid",
                        "befolkning_4h" = "Befolkning innen 4 timers kjøretid"
                        )

categorical_vars <- list("landskap" = "Hoved landskapstype i planområde")



area_breaks <- c(1000, 2000, 5000, 25000, 50000, 100000, 500000, 1000000, 5000000, 10000000)


reserve_breaks <- c(0.0001, 0.00045, 0.0006)

tetthet_ref <- 0.6
 

kodeliste_arealbruk <- read.csv(file.path(workdir, "kodeliste_arealbruk.csv"), header = FALSE, sep = ";")
names(kodeliste_arealbruk) <- c("kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kodeliste_arealformal <- read.csv(file.path(workdir, "kodeliste_arealformal.csv"), header = FALSE, sep = ";")
names(kodeliste_arealformal) <- c("kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kommuneplan_arealformal_koder <- rbind(kodeliste_arealbruk, kodeliste_arealformal)
