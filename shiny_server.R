library(leaflet)
library(RColorBrewer)
library(scales)
library(lattice)
library(dplyr)
library(sf)
library(ggplot2)
library(gt)

# Todos:
# no plots for binary variables
# full pop-up
# all variables for all aggregation levels


# Leaflet bindings are a bit slow; for now we'll just sample to compensate
zipdata <- allzips

# By ordering by centile, we ensure that the (comparatively rare) SuperZIPs
# will be drawn last and thus be easier to see
#zipdata <- zipdata[order(zipdata$areal_m2),]

function(input, output, session) {

  ## Interactive Map ###########################################
  select_mapdata <- reactive({
    reserve_data[[input$aggregation]]
    })

  get_variable <- reactive({
    if(input$aggregation == "planomrade") {
      input$variable_omrade
    } else if(input$aggregation == "kommune") {
      input$variable_kommune
    } else if(input$aggregation == "fylke") {
      input$variable_fylke
    }
  })
  
  # Create the map
  output$map <- renderLeaflet({
    leaflet() %>%
      addTiles() %>%
      setView(lng = 6.85, lat = 68.45, zoom = 4)
  })

  # A reactive expression that returns the set of geometry centroids that are
  # in bounds right now
  centroidsInBounds <- reactive({
    zipdata <- select_mapdata()
    if (is.null(input$map_bounds))
      return(zipdata[FALSE,])
    bounds <- input$map_bounds
    latRng <- range(bounds$north, bounds$south)
    lngRng <- range(bounds$east, bounds$west)

    subset(zipdata,
      latitude >= latRng[1] & latitude <= latRng[2] &
        longitude >= lngRng[1] & longitude <= lngRng[2])
  })

  # Precalculate the breaks we'll need for the two histograms
  centileBreaks <- hist(plot = FALSE, log(allzips$fid), breaks = 20)$breaks

  output$histCentile <- renderPlot({
    plot_var <- get_variable()
    dat <- centroidsInBounds() %>% st_drop_geometry() %>% select(navn, {{plot_var}})
    var_title <- gsub(" i dekar", "", names(vars[[input$aggregation]])[grep(plot_var, vars[[input$aggregation]])])
    ylims <- range(dat[[plot_var]])
    # If no zipcodes are in view, don't plot
    if (nrow(dat) == 0 | plot_var %in% categorical_variables)
      return(NULL)

    ggplot(data=dat %>% arrange(desc(.data[[plot_var]])) %>% slice_head(n=10), aes(x=as.factor(fid))
    ) +
      geom_bar(aes_string(x=paste0("reorder(as.factor(navn), 1/",plot_var,")"),fill=plot_var, y=plot_var), stat="identity") +
      # xlab("Kommunenummer") +
      theme(axis.text.x = element_text(angle = 45, vjust = 0.5),
            axis.title.x = element_blank(),
            axis.title.y = element_blank()) +
      scale_fill_continuous(low="white",
                            high="purple",
                            na.value="white",
                            guide="none",
                            #name=var_title,
                            limits=ylims)
    
    #hist(log(centroidsInBounds()$fid),
    #  breaks = centileBreaks,
    #  main = "Areal m2 (synlige utsnitt)",
    #  xlab = "Persentiler",
    #  #xlim = range(allzips$areal_m2),
    #  col = '#00DD00',
    #  border = 'white')
  })

  output$summary_table <- render_gt({
    # Show a summary table for the variable
    var <- get_variable()
    #if (nrow(centroidsInBounds()) == 0)
    #  return(NULL)
    if (var %in% categorical_variables)
      return(NULL)
    var_title <- gsub(" i dekar", "", names(vars[[input$aggregation]])[grep(var, vars[[input$aggregation]])])
    var_vect <- as.vector(as.data.frame(select_mapdata() %>% st_drop_geometry())[,var])
    hp1 <- data.frame(label=c(paste0("Minimum i ", input$aggregation, "ne"),
                              paste0("Median av ", input$aggregation, "ne"),
                              paste0("Gjennomsnitt av ", input$aggregation, "ne"),
                              paste0("Maksimum i ", input$aggregation, "ne"),
                              paste0("Areal sum total"),
                              paste0("Antall ", input$aggregation, "r med overlapp")),
                      value=c(sprintf("%0.3g daa", min(var_vect, na.rm=TRUE)),
                              sprintf("%0.3g daa", median(var_vect, na.rm=TRUE)),
                              sprintf("%0.3g daa", mean(var_vect, na.rm=TRUE)),
                              sprintf("%0.3f daa", max(var_vect, na.rm=TRUE)),
                              sprintf("%0.3g km2", sum(var_vect, na.rm=TRUE)/1000),
                              sum(ifelse(var_vect > 0, 1, 0)))) %>% 
      gt() %>% 
      tab_header(title = var_title) %>% 
      tab_options(column_labels.hidden = TRUE, table.font.size = 10) %>%
      opt_vertical_padding(scale=0.25)
  })

  # This observer is responsible for maintaining the circles and legend,
  # according to the variables the user has chosen to map to color and size.
  observe({
    aggregation <- input$aggregation
    colorBy <- get_variable()
    mapdata <- select_mapdata()
    var_title <- gsub(" i dekar", "", names(vars[[input$aggregation]])[grep(colorBy, vars[[input$aggregation]])])
    
    if (is.numeric(mapdata[[colorBy]])) {
      # Colorize categorical continuous, numeric variables
      colorData <- mapdata[[colorBy]]
      pal <- colorNumeric(palette = "viridis", colorData)
      #pal <- colorQuantile(palette = "viridis", colorData, 7)
    } else {
      # Colorize categorical variables
      colorData <- as.factor(mapdata[[colorBy]])
      pal <- colorFactor("viridis", colorData)
    }

    if (aggregation != "planomrade") {leafletProxy("map", data = mapdata) %>%
        clearShapes() %>%
        addPolygons(stroke = FALSE,
                    #smoothFactor = 0.2,
                    fillOpacity = 0.7,
                    layerId=~fid,
                    color = pal(colorData)
                    ) %>%
      addLegend("bottomleft", pal=pal, values=colorData, title=colorBy,
        layerId="colorLegend") 
      }
    else {leafletProxy("map", data = mapdata) %>%
        clearShapes() %>%
        addCircles(~longitude, ~latitude, radius=~sqrt(areal_m2)/pi,
                   layerId=~fid,
                   stroke=FALSE, fillOpacity=0.7, fillColor=pal(colorData)) %>%
        addLegend("bottomleft", pal=pal, values=colorData, title=var_title,
                  layerId="colorLegend")}
  })

  output$tomtereserve_summary_table <- render_gt(
    reserve_data[["planomrade"]] %>% 
    st_drop_geometry() %>%
    filter(tomtereserve_daa > 0) %>%
    mutate(tomtereserve_regulert_daa = ifelse(plankategori == 'Reguleringsplan', tomtereserve_daa, ifelse(is.na(tomtereserve_regulert_daa),0,tomtereserve_regulert_daa)),
           tomtereserve_uregulert_daa = ifelse(plankategori == 'Reguleringsplan', 0, ifelse(is.na(tomtereserve_regulert_daa), tomtereserve_daa, tomtereserve_daa - tomtereserve_regulert_daa))) %>%
    group_by(plankategori) %>%
    summarise(tomtereserve_regulert_km2=sum(as.double(tomtereserve_regulert_daa)/1000.0),
              tomtereserve_uregulert_km2=sum(as.double(tomtereserve_uregulert_daa)/1000.0),
              tomtereserve_km2=sum(as.double(tomtereserve_daa)/1000.0),
              tomtereserve_antall_boliger=sum(tomtereserve_antall_boliger)) %>%
    add_row(plankategori = "Estimert ved datamangel",
            tomtereserve_regulert_km2 = 0,
            tomtereserve_uregulert_km2 = 0,
            tomtereserve_km2=sum(reserve_data[["kommune"]]$tomtereserve_estimert_daa_sum)/1000.0,
            tomtereserve_antall_boliger=as.integer((sum(reserve_data[["kommune"]]$tomtereserve_estimert_daa_sum)*0.75))) %>%
    gt(rowname_col = "plankategori") %>%
    tab_stubhead(label = "Datakilde") %>%
    cols_label(#plankategori = "Plandata",
      tomtereserve_regulert_km2 = html("<center>Regulert<br><font size='-1'>km<sup>2</sup></font></center>"),
      tomtereserve_uregulert_km2 = html("<center>Uregulert<br><font size='-1'>km<sup>2</sup></font></center>"),
      tomtereserve_km2=html("<center>Total<br><font size='-1'>km<sup>2</sup></font></center>"),
      tomtereserve_antall_boliger=html("<center>Antall<br>fritidsbolig<br><font size='-1'>n</font></center>")) %>%
    tab_spanner(
      label = "Tomereserve",
      columns = c(tomtereserve_regulert_km2, tomtereserve_uregulert_km2, tomtereserve_km2, tomtereserve_antall_boliger)
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
    fmt_number(columns=c(tomtereserve_regulert_km2, tomtereserve_uregulert_km2, tomtereserve_km2),
               rows = everything(),
               decimals = 2,
               dec_mark=",",
               sep_mark=" ") %>%
    fmt_integer(columns=c(tomtereserve_antall_boliger), rows = everything(), sep_mark=" ")
  )
  
  # Show a popup at the given location
  format_attribute <- function(selected_zip, aggregation_level) {
    var_title <- toString(paste0(aggregation_level_name[aggregation_level], ": ", as.integer(selected_zip$fid)), collapse="")
    data.frame(labels=names(vars[[aggregation_level]]),
               values=as.vector(unlist(as.data.frame(selected_zip %>%
                                                       st_drop_geometry())[vars[[aggregation_level]]]))) %>%
      gt() %>% 
      tab_header(title = var_title) %>% 
      tab_options(column_labels.hidden = TRUE,
                  table.font.size = 10,
                  container.width = px(100),
                  container.height = px(200)
                  ) %>%
      opt_vertical_padding(scale=0.25) %>% as_raw_html()

    # content <- sapply(names(vars[[aggregation_level]]), function(column) {
    #   #print(column)
    #   column_name <- as.character(vars[[aggregation_level]][[column]])
    #   if (column_name %in% names(selected_zip)) {
    #   column_value <- selected_zip[[column_name]]
    #   if (is.null(column_value) | is.na(column_value)) {
    #     value <- ""
    #   } else if (is.numeric(column_value)) {
    #     value <- sprintf("%g", column_value)
    #   } else if (!is.numeric(column_value)) {
    #     value <- as.character(column_value)
    #   }
    #   paste0(column, ": ", value, tags$br())
    #   }})
    # #print(content)
    # if (length(content) > 0) {
    #   content
    # } else {
    #   " "
    # }
  }
  

  showZipcodePopup <- function(fid, lat, lng) {
    selectedZip <- select_mapdata()[select_mapdata()$fid == fid,]
    content <- as.character(tagList(
      tags$strong(aggregation_level_name[input$aggregation], as.integer(selectedZip$fid)),
      HTML(toString(format_attribute(selectedZip, input$aggregation), sep="\n"))
    ))
    leafletProxy("map") %>% addPopups(lng, lat, content, layerId = fid)
  }

  # When map is clicked, show a popup with city info
  observe({
    leafletProxy("map") %>% clearPopups()
    event <- input$map_shape_click
    if (is.null(event))
      return()

    isolate({
      showZipcodePopup(event$id, event$lat, event$lng)
    })
  })


  ## Data Explorer ###########################################

  observe({
    cities <- if (is.null(input$states)) character(0) else {
      filter(cleantable, plankategori %in% input$plankategori) %>%
        `$`('kommunenummer') %>%
        unique() %>%
        sort()
    }
    stillSelected <- isolate(input$cities[input$cities %in% cities])
    updateSelectizeInput(session, "cities", choices = cities,
      selected = stillSelected, server = TRUE)
  })

  observe({
    zipcodes <- if (is.null(input$states)) character(0) else {
      cleantable %>%
        filter(State %in% input$states,
          is.null(input$cities) | City %in% input$cities) %>%
        `$`('Zipcode') %>%
        unique() %>%
        sort()
    }
    stillSelected <- isolate(input$zipcodes[input$zipcodes %in% zipcodes])
    updateSelectizeInput(session, "zipcodes", choices = zipcodes,
      selected = stillSelected, server = TRUE)
  })

  observe({
    if (is.null(input$goto))
      return()
    isolate({
      map <- leafletProxy("map")
      map %>% clearPopups()
      dist <- 0.5
      fid <- input$goto$fid
      lat <- input$goto$lat
      lng <- input$goto$lng
      showZipcodePopup(fid, lat, lng)
      map %>% fitBounds(lng - dist, lat - dist, lng + dist, lat + dist)
    })
  })

  output$attributetable <- DT::renderDataTable(server = FALSE, {
    df <- select_mapdata() %>%
      st_drop_geometry() %>%
      #filter(
        #areal_m2 >= input$minScore,
        #areal_m2 <= input$maxScore,
        #is.null(input$data_filter_kommuner) | kommunenummer %in% input$data_filter_kommuner,
        #is.null(input$data_filter_plankategori) | plankategori %in% input$data_filter_plankategori
      #) %>%
      mutate(Action = paste('<a class="go-map" href="" data-lat="', latitude, '" data-long="', longitude, '" data-fid="', fid, '"><i class="fa fa-crosshairs"></i></a>', sep=""))
    action <- DT::dataTableAjax(session, df, outputId = "attributetable")

    DT::datatable(df,
                  extensions = c('Buttons'),
                  options = list(dom='Bfrtip',
                                 paging = TRUE,
                                 scrollX=TRUE, 
                                 #scrollY=TRUE, 
                                 searching = TRUE,
                                 ordering = TRUE,
                                 pageLength=25,
                                 lengthMenu=c(10,25,50,100,500),
                                 ajax = list(url = action),
                                 buttons = list(
                                   list(extend='csv',
                                        text="Last ned i CSV",
                                        filename="data",
                                        exportOptions=list(
                                          modifier=list(
                                            page="all"))))),
                  escape = FALSE
                  )
  })
}
