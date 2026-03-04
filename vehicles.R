#===========================================================================
# Swiss Vehicles (ASTRA) with R Polars
#===========================================================================

# Get Polars from r-multiverse
# Sys.setenv(NOT_CRAN = "true")
# install.packages("polars", repos = "https://community.r-multiverse.org")

library(polars)
library(glue)

# Manually adapt:
# - TYPE: "stock" (all vehicles at status) or "new" (newly registered vehicles in year)
# - STATUS: year like "2025" for TYPE == "new" or date string like "20260101" for TYPE == "stock"
# - OUTFILE: output parquet file path
# - COMPRESSION: parquet compression ("snappy", "gzip", "zstd")
# - COLS: Which columns to fetch (only subset listed)

TYPE <- "stock"  # "new" or "stock"
STATUS <- "20260101"  # if TYPE == "new" integer like 2025, else string like "20260101"
OUTFILE <- glue("{STATUS}_{TYPE}.parquet")
COMPRESSION <- "zstd"  # "snappy", "gzip", "zstd" etc.
COLS <- c(
  "Fahrzeugart",
  "Karosserieform",
  "Marken_Code",
  "Marke",
  "Marke_und_Typ",
  "Typ1",
  "Typ2",
  "Typ3",
  "Typ4",
  "Farbe",
  "Sitzplätze",
  "Leergewicht",
  "Gesamtgewicht",
  "Hubraum",
  "Leistung",
  "Treibstoff",
  "Erstinverkehrsetzung_Jahr",
  "Erstinverkehrsetzung_Monat",
  "Erstinverkehrsetzung_Kanton",
  "Schildfarbe",
  "Schildart",
  "Inverkehrsetzung_Status_Code",
  "Inverkehrsetzung_Kanton",
  "PLZ",
  "Staat_Code"
)

# The URL is derived from above info
MAIN <- "https://opendata.astra.admin.ch/ivzod/1000-Fahrzeuge_IVZ/"
SUB <- list(
  stock = "1300-Fahrzeugbestaende/1320-Datensaetze_monatlich/1323-Vorjahrsdaten",
  new = "1200-Neuzulassungen/1210-Datensaetze_monatlich/1213-Vorjahresdaten"
)
NAME <- list(stock = "BEST", new = "NEUZU")

(URL <- glue("{MAIN}{SUB[TYPE]}/{NAME[[TYPE]]}-{STATUS}.txt"))

df <- (
  pl$scan_csv(
    URL,
    separator = "\t",
    try_parse_dates = TRUE,
    schema_overrides = list("Leistung" = pl$String),  # In older years, values like "1'000"
    quote_char = NULL,
  )
  # $head(100)
  $select(COLS)
  $with_columns(
    pl$col("Leergewicht")$cast(pl$Float64),
    pl$col("Gesamtgewicht")$cast(pl$Float64),
    pl$col("Hubraum")$cast(pl$Float64),
    pl$col("Leistung")$str$replace("'", "")$cast(pl$Float64),
  )
)

system.time(
  df$sink_parquet(OUTFILE, compression=COMPRESSION)
)


#===========================================================================
# Peak into data
#===========================================================================

(
  pl$scan_parquet(glue("*{TYPE}*.parquet"), include_file_paths="status")
  $head()
  $collect()
) |>
  as.data.frame()

(
  pl$scan_parquet(glue("*{TYPE}*.parquet"), include_file_paths="status")
  $with_columns(pl$col("status")$str$slice(0,  nchar(STATUS)))
  $group_by(c("Fahrzeugart", "status"))
  $len()
  $sort(c("status", "len"), descending=TRUE)
  $head(10)
  $collect()
)
