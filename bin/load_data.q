// q script for loading data

// imports
// NOTE: see https://stackoverflow.com/questions/50053816/getting-the-location-of-the-current-file-in-q-kdb
.load.FILE_PATH:{[]:value[.z.s]}[];
@[system;"l ",1_string ` sv (first ` vs hsym `$.load.FILE_PATH[6];`..;`src;`q;`io.q);{[]exit 1}]

// script
show "writing kdb+ dataset to ",string .io.TRIPS_TABLEH;
st:.z.p;
@[.io.load;(::);{[]exit 1}];
show "kdb+ dataset write executed in ",string[.z.p - st]," seconds";

exit 0
