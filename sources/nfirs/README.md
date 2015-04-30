# National Fire Incident Reporting System ([NFIRS](http://www.usfa.fema.gov/data/nfirs/index.html)) Data

FireCARES uses NFIRS data to calculate community risk. Below are notes on how FireCARES uses NFIRS to generate
community risk scores.

## Obtaining NFIRS Data ##

According to the NFIRS help desk, there is no public location where you can download the entire NFIRS data set.  To
obtain NFIRS data, send an email to fema-nfirshelp@fema.dhs.gov.  The data will be sent on CDs/DVDs broken up by year.
Only the most recent year includes fire and EMS incidents, all previous years will be filtered to only fire incidents.

## Custom Tables ##

**buildingfires**: NFIRS data filtered to only building fires.

**census_block_groups_2010**: A geospatial table of the 2010 census block groups.  Block group ids are added on the `incidentaddress`
table as a foreign key in a field called `bkgpidfp10`.  The foreign key ties to the `geoid10` field on the `census_block_groups_2010` table.

**census_block_groups_2000**: A geospatial table of the 2000 census block groups.  Block group ids are added on the `incidentaddress`
table as a foreign key in a field called `bkgpidfp00`.  The foreign key ties to the `bkgpidfp00` field on the `census_block_groups_2000` table.

**usgs_stateorterritoryhigh**: A high-resolution geospatial table of US States and territories.  Data obtained from http://services.nationalmap.gov/arcgis/rest/services/govunits/MapServer/18.

**addresses_to_geocode**: A staging table for geocoding addresses.

## Custom Fields ##

**incidentaddress.geom**: The geom field is an unprojected geospatial point added to the `incidentaddress` table that represents the location
of the address.  This field has been populated at a national scale, therefore the match quality may vary from address to address.

**incidentaddress.bkgpidfp00**: The 2000 Census block group, populated by a spatial join with **census_block_groups_2000**. Can be joined with `census_block_groups_2000.bkgpidfp00`.

**incidentaddress.bkgpidfp10**: The 2010 Census block group, populated by a spatial join with **census_block_groups_2010**. Can be joined with `census_block_groups_2010.geoid10`.


## Links ##

* [NFIRS 5.0 Design Documentation](https://www.nfirs.fema.gov/documentation/design/NFIRS_5.0_Design_Documentation_1-2013.pdf)
