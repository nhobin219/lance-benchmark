// q script for loading data

// imports
// NOTE: see https://stackoverflow.com/questions/50053816/getting-the-location-of-the-current-file-in-q-kdb
.query.FILE_PATH:{[]:value[.z.s]}[];
@[system;"l ",1_string ` sv (first ` vs hsym `$.query.FILE_PATH[6];`..;`src;`q;`io.q);{[]exit 1}]

// NOTE: load the database
@[system;"l ",1_string ` sv (first ` vs hsym `$.query.FILE_PATH[6];`..;`data;`kdb;`);{[]exit 1}]

// NOTE: we need the column filter to get q to actually read the table into memory
.query.QUERY1:"select from trips where id>-1";
.query.QUERY2:"select VendorID, tpep_pickup_datetime, trip_distance from trips where trip_distance > 10";

.query.time:{[query;times]
    show query;
    st:.z.p;
    result:value query;
    min_delta:(.z.p - st)%1000000;
    show result;
    1 "First \"",query,"\" executed in ",string[min_delta]," milliseconds\n";
    do[times;
        st:.z.p;
        result:value query;
        delta:(.z.p - st)%1000000;
        if[delta<min_delta;
            min_delta:delta]];

    1 "Minimum execution time: ",string[min_delta]," milliseconds\n\n";
    :(delta;min_delta);
 }

// NOTE: for information on how q utilized os data caching see: https://stackoverflow.com/questions/19241444/how-does-q-cache-data
.query.time[.query.QUERY1;10];
.query.time[.query.QUERY2;10];

exit 0;
