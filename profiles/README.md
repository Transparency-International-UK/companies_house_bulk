### Companies House basic profile bulk files

This folder contains the Makefile which allows to download the files and 
upload them in a postgres database with a simple command. 

#### How to use

- cd into this directory
- insert the credentials to connect to the postgres db in `database.mk`
- invoke the Makefile recipes from the command line:
    - `make all` to launch in one command all separate recipes. 
    - OR: `make download`, `make unpack` and `make load`. You can remove 
    all zip files at the end with `make clean`.
      
#### To dos

The files are not cleaned in any way before being inserted. It could be worthwhile 
to write a parser that reads line by line and normalises the names and country names using [`countrynames`](https://pypi.org/project/countrynames/) and 
[`fingerprints`](https://pypi.org/project/fingerprints/). 