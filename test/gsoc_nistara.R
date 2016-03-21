

# =========================================================================
# Test code from github
# Site: https://github.com/rstats-gsoc/gsoc2016/wiki/Managing-and-visualizing-movement-data-with-PostGIS-and-R
# =========================================================================

library("spacetime")
library("sp")

data(fires)
fires$X <- fires$X * 100000
fires$Y <- fires$Y * 100000
fires$Time <- as.POSIXct(as.Date("1960-01-01")+(fires$Time-1))

names(fires)

coordinates(fires) <- c("X", "Y")
proj4string(fires) <- CRS("+init=epsg:2229 +ellps=GRS80")
plot(fires, pch = 3)

# Subset points within X = 6400000 and 6500000, Y = 1950000 and 2050000, and during the 90s:

(subfires <- subset(fires, coordinates(fires)[, 1] >= 6400000 
                    & coordinates(fires)[, 1] <= 6500000 
                    & coordinates(fires)[, 2] >= 1950000 
                    & coordinates(fires)[, 2] <= 2050000 
                    & fires$Time >= as.POSIXct("1990-01-01") 
                    & fires$Time < as.POSIXct("2000-01-01")))

rect(6400000, 1950000, 6500000, 2050000, border = "red", cex = 10)
points(subfires, col = "green")


# =========================================================================
# Test: Build SQL function SQL function that select the points
# within a spatio-temporal window
# =========================================================================

# Packages required:
# =================
library(RPostgreSQL)
library(rgdal)
library(rpostgis)

# Note: 
# 1.  The package 'rgdal' was installed from a package archive downloaded via 
#     http://www.kyngchaos.com/software/frameworks, because the PostgreSQL driver
#     was otherwise absent in my original 'rgdal' installation - confirmed by  
#     executing ogrDrivers(). This is a common problem, and this solution can be 
#     provided within the package documentation
# 2.  PostgreSQL was installed, and a user with my OS username (nistara) was created
# 3.  The password for the PostgreSQL user 'nistara' was stored in the .pgpass file,
#     in the format:
#         hostname:port:database:username:password
#         source: http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
# 
#     The above steps help us access and use POstgreSQL and PostGIS completely through R, 
#     without leaving the R environment, or having to enter passwords, etc.



# Data: fires -------------------------------------------------------------
# =========================================================================

data(fires)
fires$X <- fires$X * 100000
fires$Y <- fires$Y * 100000
fires$Time <- as.POSIXct(as.Date("1960-01-01")+(fires$Time-1))
coordinates(fires) <- c("X", "Y")
proj4string(fires) <- CRS("+init=epsg:2229 +ellps=GRS80")



# Create database to send SpatialPointsDataFrame (fires) to: --------------
# =========================================================================

system("createdb fires") # creates database named 'fires'
# system("dropdb fires") # drops/deletes created databse if needed



# Send the SpatialPointsDataFrame table to database: ----------------------
# =========================================================================

# Create data source string to use for the writeOGR function
dbname <- 'fires'
user <- 'nistara'
host <- 'localhost'
OGRstring <- paste("PG:dbname=", dbname, " user=", user, " host=localhost", sep = "")
# print(OGRstring)


# Write 'fires' to the database created above, withthe table name 'fires'
writeOGR(fires, OGRstring, "fires", driver = "PostgreSQL")



# Connect to database 'fires' with the PostgreSQL driver ------------------
# =========================================================================

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="fires", host="localhost", port="5432", user="nistara")
dbListTables(con) # fires is there now
dbListFields(con, "fires") # There are 3 fields or columns (ogc_fid, wkb_geometry, and time)

dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns 
                    WHERE table_name = 'fires'")
# The data types of the columns in the 'fires' table:
#     column_name           data_type
# 1      ogc_fid              integer
# 2 wkb_geometry                bytea
# 3         time    character varying


# Note: We would like to index the columns above, to facilitate database querying.
#       (ogd_fid was created by writeOGR, and doesn't need to be indexed)
# 1.  The vector geometries of the SpatialPointsDataFrame 'fires' is represented by 
#     WKT (Well KNown Text). Its binary equivalent is the WKB (Well Kknown Binary), 
#     and it is this format that is used to transfer and store the vector geometry
#     information in databases. 
#     The coordinate information from 'fires' was thus stored as a WKB, and we can 
#     access or query it easily once we convert it to the WKT format. 
#
# 2.  The time information from 'fires' is stored as  character varying, and we would
#     like to convert it to a 'timestamp', so that we can query it easily. 
#
# 3.  Lastly, we create indexes for both time and point data.
#
#     For creating indexes, and for converting the date field to timestamp, we use
#     PostGIS, which an be enabled by creating extensions for it.



# Create extensions to be able to use PostGIS with this database ----------
# =========================================================================

dbGetQuery(con, "CREATE EXTENSION POSTGIS") 
dbGetQuery(con, "CREATE EXTENSION btree_gist") # This extension was created so that
# I could create the index for wkb_geometry using PostGIS



# Create index for wkb_geometry -------------------------------------------
# =========================================================================

pgIndex(con, "fires", "wkb_geometry", "geom_idx", method = "gist")

# We use "GiST" as the type of index here because vector points are irregular
# and not one dimensional
# http://postgis.net/docs/using_postgis_dbmanagement.html#idp64922304



# Create index for time ---------------------------------------------------
# =========================================================================

# First, convert the date filed to a timestamp
pgAsDate(con, "fires", date = "time", tz = NULL, display = TRUE, exec = TRUE)

# Create index for the time
pgIndex(con, "fires", "time", "idx_time", method = "btree")

# We use B-Trees as the kind of index since time is one-dimensional, and can be 
# sorted along one axis 
# http://postgis.net/docs/using_postgis_dbmanagement.html#idp64922304



# Disconnecting from database ----------------------------------------------
# =========================================================================
dbDisconnect(con)



# Create SQL function for querying database -------------------------------
# =========================================================================

sql_query <- function(x_min, x_max, y_min, y_max, time_min, time_max, con) {
  query <- sprintf("SELECT ogc_fid, ST_X(wkb_geometry) As X, ST_Y(wkb_geometry) As Y, time
                        FROM fires
                        WHERE ST_X(wkb_geometry) >= %s AND 
                        ST_X(wkb_geometry) <= %s AND
                        ST_Y(wkb_geometry) >= %s AND
                        ST_Y(wkb_geometry) <= %s AND
                        time >= '%s' AND time < '%s'", 
                    x_min, x_max, y_min, y_max, time_min, time_max)
            
  data_subset <- dbGetQuery(con, query)
}
 


# Test sql_query function -------------------------------------------------
# =========================================================================

x_min <- 6400000
x_max <- 6500000
y_min <- 1950000
y_max <- 2050000
time_min <- "1990-01-01"
time_max <- "2000-01-01"


# Connecting to database again:
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="fires", host="localhost", port="5432", user="nistara")

fires_subset <- sql_query(x_min, x_max, y_min, y_max, time_min, time_max, con)


# Note: The above is a simple query function. If we are creating a package, we could do 
# the above in different ways, e.g. make a function which creates a query; this query 
# in turn being a parameter for the sql_query function above (instead of feeding the ranges 
# in it directly). We could also make a function to create a connection. Other variations include 
# specifing fields for the wkb_geomtery and time columns (because not all databases will
# have the same column names).





# Plotting original and sql subsets ---------------------------------------
# =========================================================================

# Convert the sql subset into a SpatialPointsDataFrame
coordinates(fires_subset) <- c("x", "y")
proj4string(fires_subset) <- CRS("+init=epsg:2229 +ellps=GRS80")

# Plot
par(mfrow=c(1,2))

plot(fires, pch = 3, col='grey')
rect(6400000, 1950000, 6500000, 2050000, border = "red", cex = 10)
points(subfires, pch = 3, col = 'red')
title("Original Subset")
plot(fires, pch = 3, col = 'grey')
rect(6400000, 1950000, 6500000, 2050000, border = "red", cex = 10)
points(fires_subset, pch = 3, col = 'red')
title("RPostgreSQL subset")

dev.off()


# Disconnect from databse -------------------------------------------------
dbDisconnect(con)



# References --------------------------------------------------------------

# http://ase-research.org/basille/rpostgis/
# http://ase-research.org/R/rpostgis/rpostgis.pdf
# http://postgis.net/docs/using_postgis_dbmanagement.html#idp64922304
# http://www.postgresql.org/docs/current/static/index.html
# https://rpubs.com/dgolicher/6373
# http://stackoverflow.com/questions/2146705/select-datatype-of-the-field-in-postgres
# http://gis.stackexchange.com/questions/151517/adding-a-column-of-wkt-format-in-the-table-postgresql
# http://dba.stackexchange.com/questions/37351/postgresql-exclude-using-error-data-type-integer-has-no-default-operator-class




