// read and write utilities for the benchmark

// NOTE: see https://code.kx.com/q/ref/file-text/#column-types-and-formats for type codes
.io.COLUMNS:(
    (`ID;"I");
    (`VendorID;"I");
    (`tpep_pickup_datetime;"Z");
    (`tpep_dropoff_datetime;"Z");
    (`passenger_count;"F");
    (`trip_distance;"F");
    (`RatecodeID;"F");
    (`store_and_fwd_flag;"S");
    (`PULocationID;"I");
    (`DOLocationID;"I");
    (`payment_type;"I");
    (`fare_amount;"F");
    (`extra;"F");
    (`mta_tax;"F");
    (`tip_amount;"F");
    (`tolls_amount;"F");
    (`improvement_surcharge;"F");
    (`total_amount;"F");
    (`congestion_surcharge;"F");
    (`airport_fee;"F")
 );

.io.DATA_DIR:hsym `$getenv`LANCE_KDB_DATA_PATH;
if[null .io.DATA_DIR;'invalid_data_path];

// HACK: currently the data is hard coded to be yellowcab trip data from 01/2022
.io.CSVH:` sv (.io.DATA_DIR;`csv;`trips.2022.01.csv);
if[not .io.CSVH~key .io.CSVH;'missing_csv_file];

.io.KDB_PATH:` sv (.io.DATA_DIR;`kdb);
.io.TRIPS_TABLEH:` sv (.io.KDB_PATH;`trips;`);

.io.load:{[]
    .io.TRIPS_TABLEH set .Q.en[.io.KDB_PATH;.io.i.readCsv[]];
 }

.io.i.readCsv:{[]
    if[not .io.CSVH~key .io.CSVH;'invalid_path];

    :(last flip .io.COLUMNS;enlist csv) 0: .io.CSVH;
 }
