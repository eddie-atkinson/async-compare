Rudimentary test of async performance in Python vs Node JS for purely IO bound work. In this case we simply download a parquet file containing the December quarters current account data for NZ, perform some light computations in memory and then write the file unchanged back as a CSV.