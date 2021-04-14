### Companies House product216 parser (appointments)

This folder contains the script to parse product216 bulk files. 

#### How to use

- cd into this directory
- create a new folder `data` where you store the unzipped prod216 files.   
- change the `DB_URI` in the `read_directors.py` file.
- from the same directory where the parser is, call `python3 read_directors.py`.


#### Goodies
The data is partially cleaned while being parsed. Names of countries and people are normalised into a _norm (normalised) 
_fp (fingerprint) columns.