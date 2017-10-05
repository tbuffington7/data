# Handling department data

For data that is currently NOT supported directly in FireCARES but could be used in a vector layer on the site, data should be uploaded to `s3://firecares-data-backup`.

Currently, there is a variety of of geojson files types that are transformed into mbtiles and stored at `s3://firecares-tiles`:

- Hydrants
- Fire stations
- Fire districts
- Response times
- etc

The data should be uploaded using the following naming conventions, replacing spaces with a single underscore:

```
/[ALPHA2 ISO COUNTRY CODE]/[ALPHA2 STATE/PROVINCE CODE]/[JURISDICTION NAME]/[ALPHA2 ISO COUNTRY CODE]-[ALPHA2 STATE/PROVINCE CODE]-[JURISDICTION NAME]-[FILE TYPE].[FILE EXTENSION]
```

The current [FILE TYPE] values include:

- `fire_hydrants`
- `fire_stations`
- `fire_districts`
- `building_footprints`
- `response_time`

In order to visualize this information in FireCARES, mbtiles must be generated and uploaded to `s3://firecares-tiles` and be referenced by the tile server

1. Copy geojson files from s3: aws s3 cp s3://firecares-data-backup/ . --exclude="*" --include "*hydrants*.geojson"  --recursive
2. cd us && cp */**/*.geojson ../  # need to have 'shopt -s globstar' option set
3. tippecanoe -r 0 -l hydrants -o hydrants.mbtiles -z18 -Z14 -f *.geojson
4. aws s3 cp hydrants.mbtiles s3://firecares-tiles/ --acl=public-read
