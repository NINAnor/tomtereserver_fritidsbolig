library(leaflet)
library(gt)

navbarPage("Tomtereserve for fritidsbolig i Norge", id="nav",

  tabPanel("Tomtereserven i grove trekk",
   div(class="outer",
  
     tags$head(
       # Include our custom CSS
       includeCSS("styles.css")
              ),
     gt_output("tomtereserve_summary_table"),
   ),
   ),
  tabPanel("Se på kart",
    div(class="outer",

      tags$head(
        # Include our custom CSS
        includeCSS("styles.css"),
        includeScript("gomap.js")
      ),

      # If not using custom CSS, set height of leafletOutput to a number instead of percent
      leafletOutput("map", width="100%", height="100%"),

      # Shiny versions prior to 0.11 should use class = "modal" instead.
      absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
        draggable = TRUE, top = 60, left = "auto", right = 20, bottom = "auto",
        width = 330, height = "auto",

        h3("Undersøkte variabler"),

        radioButtons("aggregation", "Aggregeringsnivå:",
                     c("Planområde" = "planomrade",
                       "Kommune" = "kommune",
                       "Fylke" = "fylke"),
                     inline=TRUE, selected = "kommune"),
        #checkboxInput("filter_tomtereserve", "Vis kun tomtereserve", TRUE),
        conditionalPanel("input.aggregation == 'planomrade'",
                         selectInput("variable_omrade", "Farge", vars[["planomrade"]], selected = "plankategori")
                         ),
        conditionalPanel("input.aggregation == 'kommune'",
                         selectInput("variable_kommune", "Farge", vars[["kommune"]], selected = "fid")
                         ),
        conditionalPanel("input.aggregation == 'fylke'",
                         selectInput("variable_fylke", "Farge", vars[["fylke"]], selected = "fid")
                         ),
        #selectInput("size", "Størrelse", vars, selected = "areal_m2"),
        #conditionalPanel("input.color == 'plankategori'",
        #  # Only prompt for threshold when coloring or sizing by superzip
        #  numericInput("threshold", "SuperZIP threshold (top n percentile)", 5)
        #),
        p("Områder med høyeste verdiene i aktuell kartutsnitt"),
        plotOutput("histCentile", height = 200),
        p("Nøkkeltall"),
        gt_output('summary_table')
      ),

      tags$div("text-align" = "center", id="cite",
        'Data sammenstillt og analysert av ', tags$em('Norsk Institutt for Naturforskning (NINA)'), ' for KDD.'
      )
    )
  ),

  tabPanel("Utforsk analyserte plandata",
    fluidRow(
      column(3,
        selectInput("data_filter_fylker", "Fylker", c("Alle fylker"="", structure(state.abb, names=state.name), "Washington, DC"="DC"), multiple=TRUE)
      ),
      column(3,
        conditionalPanel("data_filter_fylker",
          selectInput("data_filter_kommuner", "Kommuner", c("Alle kommuner"=""), multiple=TRUE)
        )
      ),
      column(3,
        #conditionalPanel("data_filter_fylker",
          selectInput("data_filter_plankategori", "Plankategori", c("Alle plankategorier"="", structure(c("Kommuneplan", "Reguleringsplan"), names=c("Kommuneplan", "Reguleringsplan"))), multiple=TRUE)
        #)
      )
    ),
    # fluidRow(
    #   column(1,
    #     numericInput("minScore", "Min score", min=0, max=100, value=0)
    #   ),
    #   column(1,
    #     numericInput("maxScore", "Max score", min=0, max=100, value=100)
    #   )
    # ),
    hr(),
    DT::dataTableOutput("attributetable")
  ),

  conditionalPanel("false", icon("location-crosshairs"))
)
