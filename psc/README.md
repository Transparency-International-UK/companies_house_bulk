### Companies House persons of significant control (psc) bulk files

This folder contains the Makefile which allows to download the files and 
upload them in a postgres database with a simple command. 

#### How to use

- cd into this directory
- insert the credentials to connect to the postgres db in `read_psc.py` variable `B_URI`
- invoke the Makefile recipes from the command line:
    - `make all` to launch in one command all separate recipes. 
    - OR: `make download`, `make unpack` and `make load`. You can remove 
    all zip files at the end with `make clean`.

#### Goodies

The data is cleaned while being parsed. Names of countries and people are normalised into a `_norm` (normalised) or
`_fp` (fingerprint) columns.