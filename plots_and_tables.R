library(RPostgres)
library(pool)
library(sf)
library("dplyr")
library(ggplot2)
library(lubridate)
library(reshape2)
library(gridExtra)
library(gt)
library(magick)
library(ggtext)
library(webshot)
#webshot::install_phantomjs()

library(scales)
library(gtable)

src_dir <- dirname(rstudioapi::getSourceEditorContext()$path)
source(paste0(src_dir, "/variables.R"))

workdir <- "/data/P-Prosjekter/15227000_kartlegging_av_tomtereserve_for_fritidsbebyggels/"
setwd(workdir)
dir.create(plot_dir, showWarnings = FALSE)


#Set connection parameters
pg_drv <- RPostgres::Postgres()

# Passord er lagret i ~/.pgpass:
# https://www.postgresql.org/docs/current/libpq-pgpass.html

pool <- dbPool(
  drv = pg_drv,
  dbname = pg_db,
  host = pg_host,
  user = pg_user,
  idleTimeout = 36000000
)

con <- poolCheckout(pool)



# https://rpostgres.r-dbi.org/reference/postgres-query.html
# https://rdrr.io/cran/postGIStools/man/get_postgis_query.html
# https://cran.r-project.org/web/packages/dplyr/vignettes/grouping.html

# https://r-graph-gallery.com/
# https://nina.sharepoint.com/SitePages/shiny-apps.aspx
# https://shiny.rstudio.com/gallery/widget-gallery.html


# columns <- dbListFields(con, name=Id(schema=schema, table = table_reserve))
# columns <- columns[!columns %in% c("geom", "bygningcentroid")]
################################################################################
# Last inn data fra databasen
plan_columns <- dbListFields(con, name=Id(schema=schema, table = table_plans))
plan_columns <- plan_columns[!plan_columns %in% c("geom", "bygningcentroid")]
plan_stats <- st_read(con, query=paste0("SELECT ", toString(plan_columns, sep=','),
" FROM \"", schema, "\".\"", table_plans, "\" WHERE bygningcentroid IS NOT NULL;"))

plan_columns_fkb <- dbListFields(con, name=Id(schema=schema, table = table_plans_fkb))
plan_columns_fkb <- plan_columns_fkb[!plan_columns_fkb %in% c("geom", "bygningcentroid")]
plan_stats_fkb <- st_read(con, query=paste0("SELECT ", toString(plan_columns_fkb, sep=','),
                                        " FROM \"", schema, "\".\"", table_plans_fkb, "\" WHERE bygningcentroid IS NOT NULL;"))
################################################################################
# Save data for shiny app
setwd("/data/Egenutvikling/15082000_egenutvikling_stefan_blumentrath/Tomtereserver_fritidsbolig_shiny")
reserve_omrader <- st_read(con, query=paste0("SELECT * FROM \"",
                                             schema,
                                             "\".\"tomtereserve_formalsomrader_shiny\";"))
saveRDS(file = "./data/reserve_omrader_centroid.rds", object = reserve_omrader)
reserve_kommuner <- st_read(con, query=paste0("SELECT * FROM \"",
                                              schema,
                                              "\".\"tomtereserve_kommuner_shiny\";"))
saveRDS(file = "./data/reserve_kommuner.rds", object = reserve_kommuner)
reserve_fylker <- st_read(con, query=paste0("SELECT * FROM \"",
                                            schema,
                                            "\".\"tomtereserve_fylker_shiny\";"))
saveRDS(file = "./data/reserve_fylker.rds", object = reserve_fylker)
################################################################################


reserve_omrader <- st_read(con, query=paste0("SELECT * FROM \"",
                                             schema,
                                             "\".\"tomtereserve_formalsomrader\";"))
reserve_kommuner <- st_read(con, query=paste0("SELECT * FROM \"",
                                              schema,
                                              "\".\"tomtereserve_kommuner\";"))
reserve_fylker <- st_read(con, query=paste0("SELECT * FROM \"",
                                            schema,
                                            "\".\"tomtereserve_fylker\";"))

reserve_stats <- st_read(con, query=paste0("SELECT *", #toString(columns, sep=','),
                                     " FROM \"", schema, "\".\"",table_reserve, "\";"))


################################################################################
# Figur 4
plan_stats_kat_n <- plan_stats %>%
  group_by(plankategori, arealformal) %>%
  summarize(n = as.integer(sum(antall_bygninger))) %>% # add count value to each row
  arrange(desc(n)) %>%
  filter(n > 1000 & !is.na(arealformal) & !is.na(n))

options(scipen=100000)
ggplot(plan_stats_kat_n,
       aes(fill=as.factor(plankategori),
           x=reorder(arealformal,n,sum),
           y=n))+
  geom_bar(stat = "identity", width=0.7)+
  theme(axis.text.x = element_text(angle = 90,
                                   vjust = 0.5,
                                   hjust=1),
        legend.position="bottom") +
  xlab("Arealformål i kommunale planer") + ylab("Anntall eksisterende fritidsbolig") +
  labs(fill = "Plankategori") # +
# scale_y_continuous(labels = comma)
ggsave(file.path(plot_dir, "plankategori_fritidsbolig.png"),
       height = 8.5,
       width = 15.6,
       units = "cm",
       dpi = 200)


sum(plan_stats$areal_m2[plan_stats$arealformal %in% c(140, 1120,1121, 1122, 1123)])/1000000.0


################################################################################
# Figur 6

plan_stats_tetthet <- plan_stats %>%
  group_by(arealformal) %>%
  summarize(antall_hytter_per_m2 = sum(antall_bygninger) / sum(areal_m2),
            n = as.integer(sum(antall_bygninger))) %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  filter(n > 1000 & antall_hytter_per_m2 > 0 & !is.na(arealformal) & !is.na(antall_hytter_per_m2))

ggplot(plan_stats_tetthet, aes(x=as.factor(reorder(as.factor(arealformal),antall_hytter_per_m2,sum)), y=antall_hytter_per_m2 #, col=plankategori
))+
  geom_bar(stat = "identity", width=0.7, fill="steelblue") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i kommunale planer") +
  ylab("Gjennomsnittlig tetthet av\neksisterende fritidsbolig (antall / m2)")
ggsave(file.path(plot_dir, "plankategori_tetthet.png"),
       height = 8.5,
       width = 15.6,
       units = "cm",
       dpi = 200)


################################################################################
# Figur 7
plan_stats_fkb$byggyear <- as.factor(as.integer(year(plan_stats_fkb$bygningdatafangstdato)/10)*10)

plan_stats_rel <- plan_stats_fkb %>%
  group_by(arealformal) %>%
  summarize(n = n()) %>%
  arrange(desc(n)) %>%
  filter(!is.na(arealformal)) %>%
  slice_head(n=20)


plan_stats_bygdate_n <- plan_stats_fkb %>%
  filter(arealformal %in% plan_stats_rel$arealformal) %>%
  group_by(byggyear, arealformal) %>%
  summarize(n = n() #as.integer(sum(antall_bygninger))
  ) %>% # add count value to each row
  #filter(bygningdatafangstdato_max >= ikrafttredelsesdato || is.na(bygningdatafangstdato_max) || is.na(ikrafttredelsesdato)) %>%
  #filter(n > 1000 & !is.na(arealformal) & !is.na(n)) %>%
  arrange(desc(n)) %>%
  filter(!is.na(arealformal) & !is.na(byggyear) & !is.na(n)) #%>%
#slice(tail(row_number(), 2))

ggplot(plan_stats_bygdate_n, aes(fill=byggyear, x=reorder(arealformal,n,sum), y=n))+
  geom_bar(stat = "identity", width=0.7)+
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i kommunale planer") + ylab("Anntall eksisterende fritidsbolig\netter registreringsdato") +
  labs(fill = "Byggeår")
ggsave(file.path(plot_dir, "plankategori_byggear.png"),
       height = 8.5,
       width = 15.6,
       units = "cm",
       dpi = 200)


################################################################################
# Figur 8
ggplot(reserve_stats, aes(x=reorder(arealformal,areal_m2,mean),
                          y=areal_m2/1000.0, fill=plankategori,
                          weight=areal_m2/1000.0))+
  geom_boxplot() +
  scale_y_log10(breaks=c(0.1, 1, 10, 100, 1000, 10000),
                labels=c("0.1", "1", "10", "100", "1000", "10000")) +
  #theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  #xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig") +
  xlab("Arealformål i kommunale planer") +
  ylab("Areal per planområde i dekar") +
  theme(legend.position="bottom") +
  labs(fill = "Plantype")
ggsave(file.path(plot_dir, "plankategori_omradeareal.png"),
       height = 8.5,
       width = 15.6,
       units = "cm",
       dpi = 200)


# # Følgende figur utgår
# ggplot(reserve_stats #[reserve_stats$plankategori == "Kommuneplan",]
#        , aes(fill=as.factor(arealformal), x=areal_m2))+
#   geom_histogram(bins=75) +
#   #stat_bin() +
#   theme(legend.position="bottom") +
#   scale_x_log10(breaks=c(100,1000,10000,100000,1000000, 10000000)) +
#   #theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
#   #xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig") +
#   xlab("Areal i m2 per planområde") + ylab("Antall områder") +
#   labs(fill = "Arealformål")
# ggsave(file.path(plot_dir, "plankategori_omradeareal_antall.png"))


################################################################################
# Tabell 2
reserve_stats_n <- reserve_omrader %>%
  st_drop_geometry() %>%
  mutate(arealformal = as.character(arealformal)) %>%
  group_by(plankategori, arealformal) %>%
  summarize(areal_daa_max = max(areal_m2) / 1000.0,
            areal_daa_median = median(areal_m2) / 1000.0,
            areal_daa_mean = mean(areal_m2) / 1000.0,
            areal_daa_min = min(areal_m2) / 1000.0,
            areal_km2_sum = sum(areal_m2) / 1000000.0
  )

reserve_stats_total <- reserve_omrader %>%
  st_drop_geometry() %>%
  summarize(areal_daa_max = max(areal_m2) / 1000.0,
            areal_daa_median = median(areal_m2) / 1000.0,
            areal_daa_mean = mean(areal_m2) / 1000.0,
            areal_daa_min = min(areal_m2) / 1000.0,
            areal_km2_sum = sum(areal_m2) / 1000000.0
  )

table_2_arealstoerrelser <- as.data.frame(reserve_stats_n) %>%
  add_row(plankategori = "Alle",
          arealformal = "Alle",
          areal_daa_max = reserve_stats_total$areal_daa_max,
          areal_daa_median = reserve_stats_total$areal_daa_median,
          areal_daa_mean =reserve_stats_total$areal_daa_mean,
          areal_daa_min = reserve_stats_total$areal_daa_min,
          areal_km2_sum = reserve_stats_total$areal_km2_sum
  ) %>%
  gt(    rowname_col = "arealformal",
         groupname_col = "plankategori"
  ) %>%
  #fmt_percent(values_perc) %>% 
  tab_header(title = "Arealstørrelser av arealformålsområder") %>% 
  opt_vertical_padding(scale=0.25) %>%
  cols_label(arealformal = "Arealformål",
             areal_daa_max = html("<center>Maximum<br><font size='-1'>daa</font></center>"),
             areal_daa_median = html("<center>Median<br><font size='-1'>daa</font></center>"),
             areal_daa_mean = html("<center>Gjennomsnitt<br><font size='-1'>daa</font></center>"),
             areal_daa_min = html("<center>Minimum<br><font size='-1'>daa</font></center>"),
             areal_km2_sum = html("<center>Sum<br><font size='-1'>km<sup>2</sup></font></center>")
             ) %>%
  fmt_number(columns=c(areal_daa_max,
                       areal_daa_median,
                       areal_daa_mean,
                       areal_daa_min,
                       areal_km2_sum),
             rows = everything(),
             decimals = 2,
             dec_mark=",",
             sep_mark=" ")
gtsave(table_2_arealstoerrelser,
       filename = "tabel_2_arealstoerrelser.html",
       path=plot_dir)

summary(reserve_stats$areal_m2)


################################################################################
# Tabell 3, Figur 11 og 12
area_breaks <- c(1000, 2000, 5000, 25000, 50000, 100000, 500000, 1000000, 5000000, 10000000)
#reserve_stats <- st_read(con, query=paste0("SELECT *", #toString(columns, sep=','),
#                                           " FROM \"", schema, "\".\"",table_reserve, "\";"))

reserve_stats$areal_m2_klasser <- findInterval(reserve_stats$areal_m2, area_breaks)

label_formated <- function(x) {
  sprintf("<%s", as.character(format(as.integer(x / 1000), big.mark = ",",digits = 0)))
}

reserve_stats_tetthet_areal <- reserve_stats %>%
  st_drop_geometry() %>%
  filter(antall_bygninger > 0) %>%
  group_by(plankategori, areal_m2_klasser) %>%
  summarize(antall_hytter_per_dekar_max = round(max((antall_bygninger / areal_m2)*1000), 3),
            antall_hytter_per_dekar_3rd_quart = round(quantile((antall_bygninger / areal_m2)*1000, 0.75), 3),
            antall_hytter_per_dekar_mean = round(mean((antall_bygninger / areal_m2)*1000), 3),
            antall_hytter_per_dekar_median = round(median((antall_bygninger / areal_m2)*1000), 3),
            antall_hytter_per_dekar_1st_quart = round(quantile((antall_bygninger / areal_m2)*1000, 0.25), 3),
            antall_hytter_per_dekar_min = round(min((antall_bygninger / areal_m2)*1000), 3)
  ) # add count value to each row

write.csv(reserve_stats_tetthet_areal,
          "./plots/tabel_3_tabel_2_arealstoerrelser.html.csv",
          row.names = FALSE)

reserve_stats_tetthet_areal %>% 
  gt()
View(reserve_stats_tetthet_areal)

dat <- reserve_stats %>% filter(antall_bygninger > 0 & tomtereserve_m2 == 0)
dat %>% st_drop_geometry() %>% group_by(plankategori, areal_m2_klasser) %>% summarize(med=median(antall_hytter_per_m2 * 1000))

text_pos <- function(x){
  return(c(y = mean(fivenum(x)[3:4]), label = median(x)))
}

ggplot(dat, aes(x=as.factor(areal_m2_klasser), y=antall_hytter_per_m2 * 1000, fill=plankategori))+
  geom_boxplot(width=0.7, outlier.alpha = 0.1) +
  scale_x_discrete(label=label_formated(append(area_breaks, max(reserve_stats$areal_m2)))) + #breaks=c(0.000001,0.0001, 0.00045, 0.0006)) +
  scale_y_log10(lim=c(0.3,2), breaks=c(0.3,0.3, 0.45, 0.6, 1, 2)) +
  #geom_text(label=paste(median(reserve_stats$antall_hytter_per_m2 * 1000)))
  #ggrepel::geom_text_repel(aes(y=median(antall_hytter_per_m2 * 1000), label = median(antall_hytter_per_m2 * 1000)), nudge_y = -0.25)
  stat_summary(geom="text", fun.data=text_pos,
               aes(label=sprintf("%1.3f", exp(..y..))),
               position=position_dodge(width=0.7)) + #"identity")+ #position_nudge(y=0.4), size=3.5               ) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Områdestørrelse i kommunale planer i dekar (1000 m2)") +
  ylab("Tetthet av eksisterende\nfritidsbolig (antall / dekar)") +
  labs(fill="Plankategori") +
  theme(legend.position="bottom")
ggsave(file.path(plot_dir, "plankategori_areal_tetthet_utbygd.png"),
       width = 15.6,
       height = 10,
       units = "cm",
       dpi = 200,)
ggplot(reserve_stats, aes(x=as.factor(areal_m2_klasser), y=antall_hytter_per_m2 * 1000, fill=plankategori))+
  geom_boxplot(width=0.7, outlier.alpha = 0.1) +
  scale_x_discrete(label=label_formated(append(area_breaks, max(reserve_stats$areal_m2)))) + #breaks=c(0.000001,0.0001, 0.00045, 0.0006)) +
  scale_y_log10(lim=c(0.001,2), breaks=c(0.001,0.1,0.3, 0.45, 0.6, 1, 2)) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Områdestørrelse i kommunale planer i dekar (1000 m2)") +
  ylab("Tetthet av eksisterende\nfritidsbolig (antall / dekar)") +
  labs(fill="Plankategori")

ggsave(file.path(plot_dir, "plankategori_areal_tetthet.png"),
       width = 15.6,
       height = 10,
       units = "cm",
       dpi = 200,)


################################################################################
# Table 5 main results
# Oversikt over tomtereserven for fritidsbolig i tilgjengelige kommunale plandata
df <- reserve_omrader %>% 
  st_drop_geometry() %>%
  filter(tomtereserve_daa > 0) %>%
  mutate(tomtereserve_regulert_daa = ifelse(plankategori == 'Reguleringsplan',
                                            tomtereserve_daa,
                                            ifelse(is.na(tomtereserve_regulert_daa),
                                                   0,
                                                   tomtereserve_regulert_daa)),
         tomtereserve_uregulert_daa = ifelse(plankategori == 'Reguleringsplan',
                                             0,
                                             ifelse(is.na(tomtereserve_regulert_daa),
                                                    tomtereserve_daa,
                                                    tomtereserve_daa - tomtereserve_regulert_daa))
         ) %>%
  group_by(plankategori) %>%
  summarise(tomtereserve_regulert_km2=sum(as.double(tomtereserve_regulert_daa)/1000.0),
            tomtereserve_uregulert_km2=sum(as.double(tomtereserve_uregulert_daa)/1000.0),
            tomtereserve_km2=sum(as.double(tomtereserve_daa)/1000.0),
            tomtereserve_antall_boliger=sum(tomtereserve_antall_boliger))

df %>%
  add_row(plankategori = "Estimert ved datamangel",
          tomtereserve_regulert_km2 = 0,
          tomtereserve_uregulert_km2 = 0,
          tomtereserve_km2=sum(
            reserve_kommuner$tomtereserve_estimert_daa_sum, na.rm=TRUE
            )/1000.0,
          tomtereserve_antall_boliger=as.integer(
            (sum(reserve_kommuner$tomtereserve_estimert_daa_sum, na.rm=TRUE)*0.75)
            )
          ) %>%
  gt(rowname_col = "plankategori") %>%
  tab_stubhead(label = "Datakilde") %>%
  cols_label(#plankategori = "Plandata",
             tomtereserve_regulert_km2 = html("<center>Regulert<br><font size='-1'>km<sup>2</sup></font></center>"),
             tomtereserve_uregulert_km2 = html("<center>Uregulert<br><font size='-1'>km<sup>2</sup></font></center>"),
             tomtereserve_km2=html("<center>Total<br><font size='-1'>km<sup>2</sup></font></center>"),
             tomtereserve_antall_boliger=html("<center>Antall<br>fritidsbolig<br><font size='-1'>n</font></center>")) %>%
  tab_spanner(
    label = "Tomereserve",
    columns = c(tomtereserve_regulert_km2,
                tomtereserve_uregulert_km2,
                tomtereserve_km2,
                tomtereserve_antall_boliger)
  ) %>%
  grand_summary_rows(columns=c(tomtereserve_regulert_km2,
                               tomtereserve_uregulert_km2,
                               tomtereserve_km2,
                               tomtereserve_antall_boliger),
                     use_seps = FALSE,
                     fns = list(
                       Total = ~sum(.)
                       ),
                     formatter = fmt_number,
                     decimals = 2,
                     dec_mark=",",
                     sep_mark=" ",
                     drop_trailing_zeros = TRUE
  ) %>%
  fmt_number(columns=c(tomtereserve_regulert_km2,
                       tomtereserve_uregulert_km2,
                       tomtereserve_km2),
             rows = everything(),
             decimals = 2,
             dec_mark=",",
             sep_mark=" ") %>%
  fmt_integer(columns=c(tomtereserve_antall_boliger),
              rows = everything(),
              sep_mark=" ")

# Totalareal for fritidsbolig
sum(reserve_omrader$areal_m2) / 1000000.0
# Tomtereserve
sum(reserve_omrader$tomtereserve_daa) / 1000.0
# Tomtereserve i kommuneplaner
sum(reserve_omrader %>% st_drop_geometry() %>% filter(plankategori == "Kommuneplan") %>%
      select(tomtereserve_daa)) / 1000.0


################################################################################
# Summarize over planstatus
reserve_omrader %>%
  st_drop_geometry() %>%
  filter(tomtereserve_daa > 0) %>%
  group_by(plankategori, planstatus) %>%
  summarize(areal_km2=sum(areal_m2)/1000000.0)


################################################################################
# Functions for plots in grid
g_legend <- function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot + theme(legend.margin=margin(t = 0, unit='cm')))) 
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box") 
  legend <- tmp$grobs[[leg]] 
  legend
} 

as_ggplot <- function(x, ...) {
  # checks ---------------------------------------------------------------------
  if (!inherits(x, c("gt_tbl", "gtsummary"))) stop("`x=` must be a 'gt' or 'gtsummary' table", call. = FALSE)
  library("magick") #, "as_ggplot")
  library("ggtext") #, "as_ggplot")
  
  # convert gtsummary to gt ----------------------------------------------------
  if (inherits(x, "gtsummary")) x <- gtsummary::as_gt(x)
  
  # save gt as image -----------------------------------------------------------
  path_gt_table_image <- fs::file_temp(ext = "png")
  gt_table_image <- gt::gtsave(x, filename = path_gt_table_image, ...)
  
  # save image in ggplot -------------------------------------------------------
  table_img <-
    magick::image_read(path_gt_table_image) %>%
    magick::image_ggplot(interpolate = TRUE)
  
  table_img
}


################################################################################
# Figur 18 ff
for(var in names(conflict_vars)) {
  var_color <- "purple"
  title <- conflict_vars[[var]]
  dat <- reserve_omrader %>% filter(tilgjengelig_stor_areal_daa > 0) %>% select({{var}}, areal_m2, tomtereserve_daa) %>% arrange(.data[[var]]) #%>% mutate(var_rank=rank(.data[[var]]), var_daa=(.data[[var]]/1000), var_log=ifelse(.data[[var]]==0,0,log(.data[[var]])))
  breaks <- c(0, mean(ifelse(is.na(dat[[var]]), 0, dat[[var]])), max(dat[[var]])) # unique(quantile(ifelse(is.na(dat[[var]]), 0, dat[[var]]))) #, c(0,0.01, 0.025, 0.05, 0.1,0.25,0.5,0.75,0.9, 0.95, 0.975, 0.99, 1.0))) # quantile(ifelse(is.na(dat[[var]]), 0, dat[[var]]), c(0,0.025, 0.05, 0.1,0.25,0.5,0.75,0.9, 0.95, 0.975, 1.0))
  cols <- viridis::viridis(length(breaks))
  print(paste(var, length(unique(breaks))))
  mp1 <- ggplot(data=dat) +
    geom_sf(aes_string(color=var), size = 2
            ) + #(.data[[var]]))) + 
    labs(tag="A") +
    scale_colour_continuous(low="white",
                            high=var_color,
                            na.value="white",
                            guide="legend",
                            # labels = ifelse(max(dat[[var]])>1000,
                            #   label_number(suffix = " K",
                            #                scale = 1/1000),
                            #   label_number_auto()),
                            name=element_blank()) + # conflict_vars[[var]]) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    #scale_colour_viridis_c(name=use_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
    #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt"))
           #size = guide_legend(title.position="top", title.hjust = 0.5))
  l1 <- g_legend(mp1)
  mp1 <- mp1 +
    theme(legend.position="none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.margin = unit(c(0.5, 2.5, 0.5, 2.5), "pt")) +
    guides("none")
  var_vect <- as.vector(as.data.frame(dat %>%
                                        st_drop_geometry())[,var])
  hp1 <- data.frame(label=c("Minimum i planområdene",
                            "Median av planområdene",
                            "Gjennomsnitt av planområdene",
                            "Maksimum i planområdene",
                            "Areal sum total",
                            "Antall planområder med overlapp"),
                    value=c(sprintf("%g daa", as.double(sprintf("%0.2f", min(var_vect)))),
                            sprintf("%g daa", as.double(sprintf("%0.2f", median(var_vect)))),
                            sprintf("%g daa", as.double(sprintf("%0.2f", mean(var_vect)))),
                            sprintf("%g daa", as.double(sprintf("%0.2f", max(var_vect)))),
                            sprintf("%g km2", as.double(sprintf("%0.2f", sum(var_vect)/1000))),
                            sum(ifelse(var_vect > 0, 1, 0)))) %>% 
    gt() %>% 
    tab_header(title = gsub(" i dekar", "", conflict_vars[var])) %>% 
    tab_options(column_labels.hidden = TRUE, table.font.size = 10) %>%
    opt_vertical_padding(scale=1.0) %>% 
    as_ggplot()
  # hp1 <- ggplot(data=dat, aes(x=var_daa)) + geom_density() + scale_x_continuous(trans = "pseudo_log", breaks=c(0, 1000, 10, 100))
  #ggplot(data=dat, aes(x=var_daa, y=areal_m2)) + geom_point() #+ scale_x_continuous(trans = "pseudo_log", breaks=c(0, 1000, 10, 100))
  
  var_kom <- sub("daa", "daa_sum", var)
  mp2 <- ggplot(data=reserve_kommuner) +
    geom_sf(aes_string(fill=var_kom), lwd=0.1) + #(.data[[var]]))) + 
    labs(tag="B") +
    scale_fill_continuous(low="white",
                          high=var_color,
                          na.value="white",
                          guide="legend",
                          # labels = ifelse(max(dat[[var]])>1000,
                          #                 label_number(suffix = " K",
                          #                              scale = 1/1000),
                          #                 label_number_auto()),
                          name=element_blank()) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    #scale_colour_viridis_c(name=use_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
    #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt"))

  l2 <- g_legend(mp2)
  mp2 <- mp2 +
    theme(legend.position="none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.margin = unit(c(0.5, 2.5, 0.5, 2.5), "pt")) +
    guides("none")
  ylims <- range(as.vector(reserve_kommuner %>%
                             st_drop_geometry() %>%
                             select(var_kom)),
                 na.rm=TRUE)
  hp2 <- ggplot(data=reserve_kommuner %>%
                  arrange(desc(.data[[var_kom]])) %>%
                  slice_head(n=10),
                aes(x=as.factor(navn))
  ) +
    geom_bar(aes_string(x=paste0("reorder(as.factor(gsub('#', ' ', gsub('\\\\s','\\n',gsub(' og', '#og', navn)))), 1/",var_kom,")"),
                        fill=var_kom,
                        y=var_kom),
             stat="identity") +
    #xlab("Kommunenummer") +
    labs(tag="D") +
    theme(axis.text.x = element_text(angle = 90,
                                     vjust = 0.5),
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95)) +
    # scale_y_continuous(labels = ifelse(max(dat[[var]])>1000,
    #                                    label_number(suffix = " K",
    #                                                 scale = 1/1000),
    #                                    label_number_auto())) +
    scale_fill_continuous(low="white", 
                          high=var_color,
                          na.value="white",
                          guide="none",
                          name=conflict_vars[[var]],
                          limits=ylims)

  mp3 <- ggplot(data=reserve_fylker) +
     geom_sf(aes_string(fill=var_kom), lwd=0.1) + #(.data[[var]]))) + 
    labs(tag="C") +
     scale_fill_continuous(low="white",
                           high=var_color,
                           na.value="white",
                           guide="legend",
                           # labels = ifelse(max(dat[[var]])>1000,
                           #                 label_number(suffix = " K",
                           #                              scale = 1/1000),
                           #                 label_number_auto()),
                           name=element_blank()) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
     # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
     #scale_colour_viridis_c(name=use_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
     #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt")) #+
    #guides(fill = guide_legend(override.aes = list(size=1))
  l3 <- g_legend(mp3)
  mp3 <- mp3 +
    theme(legend.position="none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.margin = unit(c(0.5, 2.5, 2.5, 2.5), "pt")) +
    guides("none")


   hp3 <- ggplot(data=reserve_fylker %>%
                   filter(!is.na(.data[[var_kom]])) %>%
                   arrange(desc(.data[[var_kom]])),
                 aes(x=as.factor(navn)) #%>% filter(is.na(var_kom))
          ) +
     geom_bar(aes_string(x=paste0("reorder(as.factor(gsub('#', ' ', gsub('\\\\s','\\n',gsub(' og', '#og', navn)))), 1/",var_kom,")"),
                         fill=var_kom,
                         y=var_kom),
              stat="identity") +
     #xlab("Fylkesnummer") +
     labs(tag="E") +
     theme(axis.text.x = element_text(angle = 90,
                                      vjust = 0.5),
           axis.title.x = element_blank(),
           axis.title.y = element_blank(),
           plot.tag = element_text(size = rel(1)),
           plot.tag.position = c(0.95, 0.95)) +
     # scale_y_continuous(labels = ifelse(max(dat[[var]])>1000,
     #                                    label_number(suffix = " K",
     #                                                 scale = 1/1000),
     #                                    label_number_auto())) +
     scale_fill_continuous(low="white",
                           high=var_color,
                           na.value="white",
                           guide="none",
                           name=conflict_vars[[var]])

     plot_grid <- arrangeGrob(mp1,
                              mp2,
                              mp3,
                              l1,
                              l2,
                              l3,
                              hp1,
                              hp2,
                              hp3,
                              nrow = 3,
                              heights=c(0.6,0.05,0.4))

     ggsave(file.path(plot_dir,
                      paste0("conflict_vars_", var, ".png")),
            plot_grid,
            height = 15.6,
            width = 23.0,
            units = "cm",
            dpi = 200)
     }

################################################################################
# Table 7
# Rangering av mulige konflikter med ulike miljøhensyn etter overlapp med
#fritidsboligområder etter størrelse på overlappende areal

sumcountif <- function(x) {
  sum(ifelse(x>0,1,0))
}

df_summary_area <- reserve_omrader %>% filter(tilgjengelig_stor_areal_daa > 0) %>%
  st_drop_geometry() %>%
  summarize(across(names(conflict_vars), sum))
df_summary_n <- reserve_omrader %>% filter(tilgjengelig_stor_areal_daa > 0) %>%
  st_drop_geometry() %>%
  summarize(across(names(conflict_vars), sumcountif))

df_summary_var <- data.frame(labels=gsub(" i dekar", "", as.vector(unlist(conflict_vars[as.vector(names(df_summary_area))]))),
                         values=sprintf("%0.3g",as.vector(unlist(df_summary_area))/1000.0),
                         values_n=sprintf("%i",as.vector(unlist(df_summary_n))),
                         values_perc=as.vector(unlist(df_summary_area)/sum(as.vector(reserve_stats$areal_m2 / 1000.0)))) %>%
  arrange(desc(values_perc)) %>%
  gt() %>%
  fmt_percent(values_perc) %>% 
  tab_header(title = "Miljøhensyn rangert etter overlapp med tomtereserve for fritidsbolig") %>% 
  tab_options(column_labels.hidden = TRUE, table.font.size = 10) %>%
  opt_vertical_padding(scale=0.25)
gtsave(df_summary_var, filename = "conflict_var_sumary.rtf", path=plot_dir)

#df_summary <- reserve_stats %>% st_drop_geometry() %>% summarize(across(names(conflict_vars), sum))
#df_summary <- data.frame(labels=as.vector(names(df_summary)), values=as.vector(unlist(df_summary))/1000000)

################################################################################
# Figures in 4.4.2
for(var in names(continous_vars)) {
  var_color <- if (var %in% c("tomtereserve_daa")) {
    "#D16103"
  } else {
    "steelblue"
  }
  bbox <- st_bbox(reserve_kommuner)
  title <- continous_vars[[var]]
  if (grepl("meter", title, fixed = TRUE)) {
      var_unit <- " m"
  } else if (grepl("minuter", title, fixed = TRUE)) {
    var_unit <- " min"
  } else {
    var_unit <- ""
  } 
  dat <- reserve_omrader %>%
    filter(tilgjengelig_stor_areal_daa > 0) %>%
    select({{var}}, areal_m2, tomtereserve_daa) %>%
    arrange(.data[[var]]) #%>%
    #mutate(var_rank=rank(.data[[var]]),
    #       var_daa=(.data[[var]]/1000),
    #       var_log=ifelse(.data[[var]]==0,0,log(.data[[var]])))
  breaks <- c(0,
              mean(ifelse(is.na(dat[[var]]), 0, dat[[var]])),
              max(dat[[var]]))
  cols <- viridis::viridis(length(breaks))
  print(paste(var, length(unique(breaks))))
  mp1 <- ggplot(data=dat) +
    geom_sf(aes_string(color=var), size = 2
    ) + 
    coord_sf(xlim = c(bbox$xmin, bbox$xmax),
             ylim = c(bbox$ymin, bbox$ymax),
             expand = TRUE) + #(.data[[var]]))) + 
    labs(tag="A") +
    scale_colour_continuous(low="white",
                            high=var_color,
                            na.value="white",
                            guide="legend",
                            name=element_blank()) + # conflict_vars[[var]]) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    #scale_colour_viridis_c(name=continous_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
    #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt"))
  #size = guide_legend(title.position="top", title.hjust = 0.5))
  l1 <- g_legend(mp1)
  mp1 <- mp1 + theme(legend.position="none",
                     axis.title.x = element_blank(),
                     axis.title.y = element_blank(),
                     plot.margin = unit(c(0.5, 2.5, 0.5, 2.5), "pt")) +
    guides("none")

  var_vect <- as.vector(as.data.frame(dat %>% st_drop_geometry())[,var])
  hp1 <- data.frame(label=c("Minimum i planområdene",
                            "Første kvartil i planområdene",
                            "Median av planområdene",
                            "Gjennomsnitt av planområdene",
                            "Tredje kvartil i planområdene",
                            "Maksimum i planområdene"),
                    value=c(sprintf(paste0("%0.3g", var_unit), min(var_vect, na.rm=TRUE)),
                            sprintf(paste0("%0.3g", var_unit), quantile(var_vect, 0.25, na.rm=TRUE)),
                            sprintf(paste0("%0.3g", var_unit), median(var_vect, na.rm=TRUE)),
                            sprintf(paste0("%0.3g", var_unit), mean(var_vect, na.rm=TRUE)),
                            sprintf(paste0("%0.3g", var_unit), quantile(var_vect, 0.75, na.rm=TRUE)),
                            sprintf(paste0("%0.3f", var_unit), max(var_vect, na.rm=TRUE)))) %>% 
    gt() %>% 
    tab_header(title = title) %>% 
    tab_options(column_labels.hidden = TRUE, table.font.size = 10) %>%
    opt_vertical_padding(scale=0.25) %>% 
    as_ggplot()
  # hp1 <- ggplot(data=dat, aes(x=var_daa)) + geom_density() + scale_x_continuous(trans = "pseudo_log", breaks=c(0, 1000, 10, 100))
  #ggplot(data=dat, aes(x=var_daa, y=areal_m2)) + geom_point() #+ scale_x_continuous(trans = "pseudo_log", breaks=c(0, 1000, 10, 100))
  
  var_kom <- if((grepl("daa", var, fixed = TRUE) | grepl("antall", var, fixed = TRUE)) & !grepl("per_daa", var, fixed = TRUE)) {
    paste0(var, "_sum")
  } else {
    paste0(var, "_avg")
  }
  mp2 <- ggplot(data=reserve_kommuner) +
    geom_sf(aes_string(fill=var_kom), lwd=0.1) + #(.data[[var]]))) + 
    labs(tag="B") +
    scale_fill_continuous(low="white", high=var_color, na.value="white", guide="legend", name=element_blank()) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    #scale_colour_viridis_c(name=continous_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
    #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt"))
  
  l2 <- g_legend(mp2)
  mp2 <- mp2 + theme(legend.position="none", axis.title.x = element_blank(), axis.title.y = element_blank(),
                     plot.margin = unit(c(0.5, 2.5, 0.5, 2.5), "pt")) + guides("none")
  ylims <- range(as.vector(reserve_kommuner %>% st_drop_geometry() %>% select(var_kom)), na.rm=TRUE)
  hp2 <- ggplot(data=reserve_kommuner %>%
                  arrange(desc(.data[[var_kom]])) %>%
                  slice_head(n=10),
                aes(x=as.factor(navn))
  ) +
    geom_bar(aes_string(x=paste0("reorder(as.factor(navn), 1/",var_kom,")"),fill=var_kom, y=var_kom), stat="identity") +
    labs(tag="E") +
    #xlab("Kommunenummer") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95)) +
    scale_fill_continuous(low="white", high=var_color, na.value="white", guide="none", name=conflict_vars[[var]], limits=ylims)
  
  mp3 <- ggplot(data=reserve_fylker) +
    geom_sf(aes_string(fill=var_kom), lwd=0.1) + #(.data[[var]]))) + 
    labs(tag="C") +
    scale_fill_continuous(low="white", high=var_color, na.value="white", guide="legend", name=element_blank()) + #, low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    # scale_colour_gradient2(low=cols[1], high=cols[3], mid=cols[2], midpoint=breaks[2]) # colours=viridis::viridis(length(breaks)), breaks=breaks) +
    #scale_colour_viridis_c(name=continous_vars[[var]], breaks=unique(breaks), #labels=quantile(.data[[var]]),
    #                       ,na.value="grey", guide="legend") +
    theme(legend.position='bottom',
          legend.box="vertical",
          legend.box.margin = unit(c(0, 0, 0, 0), "pt"),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95),
          legend.key.width=unit(10, "pt"),
          legend.key.height=unit(10, "pt")) #+
  #guides(fill = guide_legend(override.aes = list(size=1))
  l3 <- g_legend(mp3)
  mp3 <- mp3 +
    theme(legend.position="none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.margin = unit(c(0.5, 2.5, 2.5, 2.5), "pt")) +
    guides("none")
  
  
  hp3 <- ggplot(data=reserve_fylker %>%
                  filter(!is.na(.data[[var_kom]])) %>%
                  arrange(desc(.data[[var_kom]])),
                aes(x=as.factor(navn)) #%>% filter(is.na(var_kom))
  ) +
    geom_bar(aes_string(x=paste0("reorder(as.factor(gsub('#', ' ', gsub('\\\\s','\\n',gsub(' og', '#og', navn)))), 1/",var_kom,")"),
                        fill=var_kom,
                        y=var_kom),
             stat="identity") +
    labs(tag="E") +
    #xlab("Fylkesnummer") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          plot.tag = element_text(size = rel(1)),
          plot.tag.position = c(0.95, 0.95)) +
    scale_fill_continuous(low="white",
                          high=var_color,
                          na.value="white",
                          guide="none",
                          name=conflict_vars[[var]])
  
  plot_grid <- arrangeGrob(mp1,
                           mp2,
                           mp3,
                           l1,
                           l2,
                           l3,
                           hp1,
                           hp2,
                           hp3,
                           nrow = 3,
                           heights=c(0.6,0.1,0.3))
  
  ggsave(file.path(plot_dir,
                   paste0("continous_vars_", var, ".png")),
         plot_grid,
         height = 15.6,
         width = 23.0,
         units = "cm",
         dpi = 200)
}




#plandata$test[is.na(plandata$test)] <- "Ingen plandata"
#table(plandata$test)


# Tomtereserve over tid
ggplot(reserve_omrader %>%
         filter(!is.na(ikrafttredelsesdato) &
                  tilgjengelig_stor_areal_daa > 0 &
                  year(ikrafttredelsesdato) > 1900),
       aes(x=year(vedtakendeligplandato),
           y=areal_m2,
           fill=plankategori))+
  geom_boxplot() #+
  #scale_y_log10(breaks=c(100,1000,10000,100000,1000000, 10000000)) +
  #theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  #xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig") +
  #xlab("Arealformål i kommunale planer") + ylab("Areal i m2 per planområde") +
  #labs(fill = "Plantype")
#test <- reserve_stats %>%
#  filter(!is.na(ikrafttredelsesdato) &
#           tilgjengelig_stor_areal_m2 > 0 &
#           year(ikrafttredelsesdato) > 1900)

summary(reserve_stats$areal_m2)

sum(ifelse(reserve_stats$antall_hytter_per_m2 > 0.0001, reserve_stats$areal_m2, 0)) / 1000000

reserve_stats_n <- reserve_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  group_by(arealformal) %>%
  filter(antall_hytter_per_m2 < 0.01 & areal_m2 > 500 & year(bygningdatafangstdato_max) < 2015 & year(bygningdatafangstdato_min) > 2010 & antall_bygninger > 0)

reserve_stats_n <- reserve_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  filter(year(bygningdatafangstdato_avg) < 2015 & year(bygningdatafangstdato_avg) > 2010 & ! is.na(antall_bygninger) & antall_bygninger > 0 & antall_hytter_per_m2 < 0.02)

min(reserve_stats_n$antall_hytter_per_m2)
summary(as.integer(reserve_stats_n$areal_m2 / reserve_stats_n$antall_bygninger))



reserve_stats$fylke <- substr(reserve_stats$kommunenummer, start = 1, stop = 2)



reserve_breaks <- c(0.0001, 0.00045, 0.0006)

reserve_stats$reserve_klasser <- ifelse(reserve_stats$tomtereserve_m2 > 0, "Tomtereserve", "Utbygd")
# reserve_stats$reserve_klasser <- ifelse(reserve_stats$tomtereserve_m2 > 1000, "Tomtereserve", "Utbygd")
#findInterval(reserve_stats$antall_hytter_per_m2, reserve_breaks)




reserve_stats_kom <- reserve_stats %>%
  # filter(plankategori == 'Kommuneplan') %>%
  # st_drop_geometry() %>%
  group_by(kommunenummer_aktuell) %>%
  summarize(land_areal = first(kommune_landareal_km2),
            fritidsbolig_areal = sum(areal_m2 / 1000000),
            areal_andel_total = sum((areal_m2 / 1000000) / kommune_landareal_km2)*100,
            areal_andel_tomtereserve = sum((tomtereserve_m2 / 1000000) / kommune_landareal_km2)*100,
            areal_andel_utbygd = sum(((areal_m2 - tomtereserve_m2) / 1000000) / kommune_landareal_km2)*100,
            plankategorier_n = n_distinct(plankategori),
            plankategorier = paste(unique(sort(plankategori)), collapse=" og "),
            antall_bygninger = as.integer(sum(antall_bygninger)),
            antall_hytter_per_m2 = mean(antall_hytter_per_m2),
            landskap = paste(unique(sort(landskap)), collapse=", "),
            myr_areal_m2 = sum(myr_areal_m2),
            fjell_areal_m2 = sum(fjell_areal_m2),
            kystsone_1_m2 = sum(kystsone_1_m2),
            kystsone_2_m2 = sum(kystsone_2_m2),
            kystsone_3_m2 = sum(kystsone_3_m2),
            naturvern_m2 = sum(naturvern_m2),
            naturvern_streng_m2 = sum(naturvern_streng_m2),
            naturvern_foreslatt_m2 = sum(naturvern_foreslatt_m2),
            naturvern_foreslatt_streng_m2 = sum(naturvern_foreslatt_streng_m2),
            naturtyper_m2 = sum(naturtyper_m2),
            naturtyper_hoy_verdi_m2 = sum(naturtyper_hoy_verdi_m2),
            befolkning_2h = mean(befolkning_2h),
            kjoereavstand_avg_2h = mean(kjoereavstand_avg_2h),
            kjoereavstand_naermeste_tettsted_minutes = mean(kjoereavstand_naermeste_tettsted_minutes),
            befolkning_3h = mean(befolkning_3h),
            kjoereavstand_avg_3h = mean(kjoereavstand_avg_3h),
            befolkning_4h = mean(befolkning_4h),
            kjoereavstand_avg_4h = mean(kjoereavstand_avg_4h),
            reindrift_funksjonsomrader_m2 = sum(reindrift_funksjonsomrader_m2),
            villrein_m2 = sum(villrein_m2),
            villrein_helar_m2 = sum(villrein_helar_m2),
            reindrift_adminomrader_m2 = sum(reindrift_adminomrader_m2),
            #infrastrukturindeks_mean = mean(infrastrukturindeks_mean),
            #infrastrukturindeks_max = mean(infrastrukturindeks_max),
            #avstand_kyst = quantile(avstand_kyst, 0.1, na.rm=TRUE),
            #hoh_perc10 = quantile(hoh, 0.1, na.rm=TRUE),
            #hoh_mean = mean(hoh),
            #hoh_perc90 = quantile(hoh, 0.9, na.rm=TRUE),
            kommune_landareal_km2 = first(kommune_landareal_km2
                                          ),
  ) # %>% # add count value to each row

reserve_stats_kommuneplan <- reserve_stats %>%
  st_drop_geometry() %>%
  #filter(plankategori == 'Kommuneplan') %>%
  group_by(kommunenummer_aktuell) %>%
  summarize(plankategori=toString(unique(plankategori), collapse="/"),
            land_areal = first(kommune_landareal_km2),
            fritidsbolig_areal = sum(areal_m2 / 1000000.0),
            areal_andel_total = sum((areal_m2 / 1000000.0) / kommune_landareal_km2)*100.0,
            areal_andel_tomtereserve = sum((tomtereserve_m2 / 1000000.0) / kommune_landareal_km2)*100.0,
            areal_andel_utbygd = sum(((areal_m2 - tomtereserve_m2) / 1000000.0) / kommune_landareal_km2)*100.0) %>%
  filter(grepl('Kommuneplan', plankategori, fixed=TRUE))
            

# Gjennomsnittlig arealandel av fritidsboligområder
dat.m <- melt(reserve_stats_kommuneplan,
              id.vars=c('kommunenummer_aktuell'),
              measure.vars=c('areal_andel_total',
                             'areal_andel_tomtereserve',
                             'areal_andel_utbygd'))
dat.m %>%                               # Summary by group using dplyr
  group_by(variable) %>%
  summarize(min = min(value),
            q1 = quantile(value, 0.25),
            median = median(value),
            mean = mean(value),
            q3 = quantile(value, 0.75),
            max = max(value))

square <- function(x) {x^2}
ggplot(dat.m #reserve_stats_kom
       , aes(y=sqrt(value)
             , x=as.factor(variable)
             , fill=as.factor(variable) #plankategorier
             ))+
  geom_boxplot(width=0.5) +
  stat_summary(geom="text", fun=quantile,
               aes(label=sprintf("%1.3f", ..y..^2)),
               position=position_nudge(x=0.4), size=3.5) +
  #scale_fill_discrete(labels=square) +
  scale_y_continuous(labels=square) +
  #xlab("Tilgjengelige plandata") + 
  ylab("Andel av areal med fritidsbolig formål\nav kommunens landareal i %") +
  scale_x_discrete(name = NULL, labels = c("Områder med\nfritidsboligformål total", "Tomtereserve for\nfritidsbolig", "Utbygde områder\nmed fritidsbolig")
  ) +
  theme(legend.position="none")


ggsave(file.path(plot_dir, "kommune_fritisbolig_areal.png"),
       width = 15.6,
       height = 10,
       units = "cm",
       dpi = 200)


reserve_stats_kom_plandat <- reserve_stats %>%
  group_by(kommunenummer_aktuell) %>%
  summarize(plankategori = paste(unique(sort(plankategori)), collapse=" og "))

table(reserve_stats_kom_plandat$plankategori)

 "antall_hytter_per_m2"                    
 "antall_bygninger"                        
 "bygningstype_uniq"                       
 "bygningsstatus_uniq"                     
 "innmalingsstatus_uniq"                   
 "bygningdatafangstdato_max"               
 "bygningdatafangstdato_avg"               
 "bygningdatafangstdato_min"               
 "bygningdatafangstdato"                   
 "landskap"                                
 "myr_areal_m2"                            
 "fjell_areal_m2"                          
 "kystsone_1_m2"                           
 "kystsone_2_m2"                           
 "kystsone_3_m2"                           
 "avstand_kystkontur_m"                    
 "naturvern_m2"                            
 "naturvern_streng_m2"                     
 "naturvern_foreslatt_m2"                  
 "naturvern_foreslatt_streng_m2"           
 "naturtyper_m2"                           
 "naturtyper_hoy_verdi_m2"                 
 "befolkning_2h"                           
 "kjoereavstand_avg_2h"                    
 "kjoereavstand_naermeste_tettsted_minutes"
 "befolkning_3h"                           
 "kjoereavstand_avg_3h"                    
 "befolkning_4h"                           
 "kjoereavstand_avg_4h"                    
 "reindrift_funksjonsomrader_m2"           
 "villrein_m2"                             
 "villrein_helar_m2"                       
 "reindrift_adminomrader_m2"               
 "infrastrukturindeks_mean"                
 "infrastrukturindeks_max"                 
 "avstand_kyst"                            
 "hoh"                                     
 "kommune_landareal_km2"                   
 "kommunenummer_aktuell"                   
 "fylke"                                   
 "areal_m2_klasser"  


# tetthet_ref <- 0.6
#  
# reserve_stats$tomte_potensial <- ifelse(as.integer(reserve_stats$areal_m2 / (1000 / tetthet_ref)) < 1, 1, as.integer(reserve_stats$areal_m2 / (1000 / tetthet_ref)))
# reserve_stats$tomtereserve <- ifelse(reserve_stats$tomte_potensial - reserve_stats$antall_bygninger < 0, as.integer(0), as.integer(reserve_stats$tomte_potensial - reserve_stats$antall_bygninger))
# 
# sum(reserve_stats$tomtereserve) 
# sum(reserve_stats$tomtereserve) 

ggplot(reserve_stats_kom, aes(x=areal_andel)) +
  geom_histogram()

ggplot(reserve_stats_kom, aes(x=naturtyper_m2/1000000)) +
  geom_histogram()

ggplot(reserve_stats_kom, aes(x=antall_bygninger)) +
  geom_histogram()

reserve_stats_fylke <- reserve_stats %>%
  group_by(fylke) %>%
  summarize(antall_bygninger = as.integer(sum(antall_bygninger)),
            fjell_areal_m2 = sum(fjell_areal_m2),
            naturtyper_m2 = sum(naturtyper_m2),
            reindrift_funksjonsomrader_m2 = sum(reindrift_funksjonsomrader_m2),
            naturvern_m2 = sum(naturvern_m2),
            villrein_m2 = sum(villrein_m2)
  ) # %>% # add count value to each row

ggplot(reserve_stats_kom, aes(x=kommunenummer, y=naturtyper_m2))+
  geom_point()
ggplot(reserve_stats_fylke, aes(x=fylke, y=naturtyper_m2))+
  geom_point()


ggplot(reserve_stats, aes(x=befolkning_2h, y=..density.., w=areal_m2))+
  geom_histogram()

ggplot(reserve_stats, aes(x=befolkning_4h, y=..density.., w=areal_m2))+
  geom_histogram()

ggplot(reserve_stats, aes(x=naturtyper_m2, y=..density.., w=areal_m2))+
  geom_histogram(binwidth=sd(reserve_stats$naturtyper_m2))

ggplot(reserve_stats[!is.na(reserve_stats$antall_bygninger),], aes(x="1", y=antall_bygninger))+
  geom_violin()

# plan_stats$ikrafttredelseyear <- as.factor(ifelse(is.na(plan_stats$ikrafttredelsesdato), "Ikke vedtatt", as.integer(year(plan_stats$ikrafttredelsesdato)/10)*10))
# plan_stats_ikraft_n <- plan_stats %>%
#   # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
#   group_by(ikrafttredelseyear, arealformal) %>%
#   summarize(n = as.integer(sum(antall_bygninger))) %>% # add count value to each row
#   #filter(bygningdatafangstdato_max >= ikrafttredelsesdato || is.na(bygningdatafangstdato_max) || is.na(ikrafttredelsesdato)) %>%
#   #filter(n > 1000 & !is.na(arealformal) & !is.na(n)) %>%
#   arrange(desc(n)) %>%
#   filter(n > 1000 & !is.na(arealformal) & !is.na(ikrafttredelseyear) & !is.na(n))
# 
# ggplot(plan_stats_ikraft_n, aes(fill=ikrafttredelseyear, x=reorder(arealformal,n,sum), y=n))+
#   geom_bar(stat = "identity", width=0.7)+
#   theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
#   xlab("Arealformål i kommunale planer") + ylab("Anntall eksisterende fritidsbolig") +
#   labs(fill = "Byggeår")

#ggplot(plan_stats_n, aes(fill=as.factor(plantype), x=reorder(arealformal,arealformal,length)))+
#  geom_bar(width=0.7)+
#  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
#  xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig") +
#  labs(fill = "Plantype")

#ggplot(plan_stats_n, aes(fill=as.factor(lovreferansetype), x=reorder(arealformal,arealformal,length)))+
#  geom_bar(width=0.7)+
#  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
#  xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig") +
#  labs(fill = "Lovreferanse")

# ggplot(plan_stats_n, aes(fill=as.factor(as.integer(year(ikrafttredelsesdato)/10)*10), x=reorder(arealformal,arealformal,length)))+
#   geom_bar( width=0.7)+
#   theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
#   xlab("Arealformål i kommunale planer") + ylab("Anntall eksisterende fritidsbolig") +
#   labs(fill = "ikrafttredelsesdato")

kp_stats <- dbGetQuery(con, paste0("SELECT 
kommunenummer 
, bygningsid 
, bygningdatafangstdato
, reguleringsplan_arealformal 
, reguleringsplan_ikrafttredelsesdato 
, kommuneplan_arealformal 
, kommuneplan_arealbruksstatus 
, planstatus 
, planbestemmelse 
, kommuneplan_ikrafttredelsesdato 
 FROM \"", schema, "\".\"", table, "\";"))

rp_formal <- kp_stats %>% group_by(reguleringsplan_arealformal)
kp_formal <- kp_stats %>% group_by(kommuneplan_arealformal)

# sjekk duplikater

# Don't map a variable to y
ggplot(kp_stats, aes(x=factor(kommuneplan_arealformal)))+
  geom_bar(stat="count", width=0.7, fill="steelblue")+
  theme_minimal()


kodeliste_arealbruk <- read.csv(file.path(workdir, "kodeliste_arealbruk.csv"), header = FALSE, sep = ";")
names(kodeliste_arealbruk) <- c("kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kodeliste_arealformal <- read.csv(file.path(workdir, "kodeliste_arealformal.csv"), header = FALSE, sep = ";")
names(kodeliste_arealformal) <- c("kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kommuneplan_arealformal_koder <- rbind(kodeliste_arealbruk, kodeliste_arealformal)


kp_stats_n <- kp_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  group_by(kommuneplan_arealformal) %>%
  mutate(n = n()) %>% # add count value to each row
  filter(n > 1000 & !is.na(kommuneplan_arealformal)) %>%
  arrange(desc(n)) #%>%
  #select(-n)

kp_stats_n <- merge(kp_stats_n, kommuneplan_arealformal_koder)
kp_stats_n$kommuneplan_arealformal_kort = substr(kp_stats_n$kommuneplan_arealformal_tekst,1,35)

ggplot(kp_stats_n, aes(x=reorder(kommuneplan_arealformal_kort,kommuneplan_arealformal_kort,length)))+
  geom_bar(width=0.7, fill="steelblue")+
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig")

ggsave(file.path(plot_dir, "fritidsbolig_kommuneplan.png"))


kp_stats_n_nybygg <- kp_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  group_by(kommuneplan_arealformal) %>%
  mutate(n = n()) %>% # add count value to each row
  filter(n > 1000 & !is.na(kommuneplan_arealformal)) %>%
  filter(bygningdatafangstdato >= kommuneplan_ikrafttredelsesdato) %>%
  arrange(desc(n)) %>%
  select(-n)

kp_stats_n_nybygg <- merge(kp_stats_n_nybygg, kommuneplan_arealformal_koder)
kp_stats_n_nybygg$kommuneplan_arealformal_kort = substr(kp_stats_n_nybygg$kommuneplan_arealformal_tekst,1,35)


ggplot(kp_stats_n_nybygg, aes(x=reorder(kommuneplan_arealformal_kort,kommuneplan_arealformal_kort,length)))+
  geom_bar(width=0.7, fill="steelblue")+
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i kommuneplan") + ylab("Anntall fritidsbolig")
ggsave(file.path(plot_dir, "fritidsbolig_reguleringsplan.png"))


rp_stats_n <- kp_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  group_by(reguleringsplan_arealformal) %>%
  mutate(n = n()) %>% # add count value to each row
  filter(n > 100 & !is.na(reguleringsplan_arealformal)) %>%
  arrange(desc(n)) %>%
  select(-n)

rp_stats_n$reguleringsplan_arealformal_kort = substr(rp_stats_n$reguleringsplan_arealformal,1,35)

ggplot(rp_stats_n, aes(x=reorder(reguleringsplan_arealformal_kort,reguleringsplan_arealformal_kort,length)))+
  geom_bar(width=0.7, fill="steelblue") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i reguleringsplan") + ylab("Anntall fritidsbolig")


rp_kp_stats_n <- kp_stats %>%
  # filter(!is.na(hindfoot_length) & !is.na(weight)) %>%
  group_by(kommuneplan_arealformal, reguleringsplan_arealformal) %>%
  mutate(n = n()) %>% # add count value to each row
  filter(n > 100 & !is.na(kommuneplan_arealformal) & !is.na(reguleringsplan_arealformal)) %>%
  arrange(desc(n)) %>%
  select(-n)

rp_kp_stats_n$planformal = paste(as.character(rp_kp_stats_n$kommuneplan_arealformal), as.character(substr(rp_kp_stats_n$reguleringsplan_arealformal,1,35)), sep="_")

ggplot(rp_kp_stats_n, aes(x=reorder(planformal,planformal,length)))+
  geom_bar(width=0.7, fill="steelblue") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  xlab("Arealformål i reguleringsplan") + ylab("Anntall fritidsbolig")

ggsave(file.path(plot_dir, "fritidsbolig_reguleringsplan.png"))


rp_tetthet <- dbGetQuery(con,"SELECT
reguleringsplan_arealformal,
vedtakendeligplandato,
sum(antall) AS antall_fritidsbygg_totalt,
sum(antall) / (sum(ST_Area(reguleringsplan_omrade)) / 10000.0) AS tetthet_fritidsbygg_sum,
max(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_max, 
avg(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_avg, 
stddev(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_stddev
FROM (SELECT 
count(*) AS antall
, reguleringsplan_omrade
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, CAST(date_part('year', reguleringsplan_vedtakendeligplandato) / 10 AS integer) * 10 AS vedtakendeligplandato
 FROM \"tomtereserver_fritidsbolig\".\"fritidsbygg_vs_plan_alle\"
GROUP BY reguleringsplan_omrade
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, CAST(date_part('year', reguleringsplan_vedtakendeligplandato) / 10 AS integer) * 10) AS x
GROUP BY reguleringsplan_arealformal, vedtakendeligplandato
;")




rp_tetthet_stats <- rp_tetthet %>%
  group_by(reguleringsplan_arealformal, vedtakendeligplandato) %>%
  filter(antall_fritidsbygg_totalt > 100)
  #arrange(desc(n)) %>%
  #select(-n)

#

  
  












surveys_abun_species %>%
  group_by(year, genus) %>%
  tally() %>%
  arrange(desc(n)) # Adding arrange just to compare with histogram




















# get_postgis_query(con, statement, geom_name = NA_character_,
#                   hstore_name = NA_character_)

kp_geo_stats <- get_postgis_query(con, paste0("SELECT * FROM \"", schema, "\".\"kommuneplaner_arealbrukomrade\";", geom_name = "geom"))

municipalities <- st_read(con,
                          query=paste0("SELECT * FROM \"",
                                       schema,
                                       "\".\"kommuner\";"))

ggplot(municipalities) +
  geom_sf(aes_string(fill="landareal_km2")) +
  scale_fill_viridis_c(na.value="grey",
                       guide="legend",
                       name="test")

plandata <- left_join(municipalities %>%
                        st_drop_geometry() %>%
                        mutate(kommunenummer_aktuell=as.character(kommunenummer)) %>%
                        select(kommunenummer_aktuell) %>%
                        group_by(kommunenummer_aktuell) %>%
                        summarize(),
                      plan_stats %>%
                        st_drop_geometry() %>%
                        select(c(kommunenummer, plankategori)) %>%
                        group_by(kommunenummer) %>%
                        arrange(kommunenummer) %>%
                        summarize(test=toString(unique(sort(plankategori)))),
                      by=c("kommunenummer_aktuell" = "kommunenummer")) %>%
  arrange(kommunenummer_aktuell)

