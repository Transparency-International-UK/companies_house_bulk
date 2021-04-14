SOURCE_DIR = data
SHELL = /usr/bin/env bash -o pipefail  # if a command fails, the pipeline of commands will fail.
PYTHON=$(VIRTUAL_ENV)/bin/python3

all: clean download unpack load

clean:
	rm -rf $(SOURCE_DIR)/*.zip

download:
	mkdir -p $(SOURCE_DIR)
	# the regex must be broken 1 to 9 and 1+0 to 16 (10 to 16)
	wget -r -nH -c -np -l 1 -A zip --accept-regex "[1-9]of18.zip" -P $(SOURCE_DIR) http://download.companieshouse.gov.uk/en_pscdata.html
	wget -r -nH -c -np -l 1 -A zip --accept-regex "1[0-6]of18.zip" -P $(SOURCE_DIR) http://download.companieshouse.gov.uk/en_pscdata.html

unpack:
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_1of18.zip >$(SOURCE_DIR)/part1.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_2of18.zip >$(SOURCE_DIR)/part2.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_3of18.zip >$(SOURCE_DIR)/part3.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_4of18.zip >$(SOURCE_DIR)/part4.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_5of18.zip >$(SOURCE_DIR)/part5.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_6of18.zip >$(SOURCE_DIR)/part6.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_7of18.zip >$(SOURCE_DIR)/part7.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_8of18.zip >$(SOURCE_DIR)/part8.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_9of18.zip >$(SOURCE_DIR)/part9.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_10of18.zip >$(SOURCE_DIR)/part10.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_11of18.zip >$(SOURCE_DIR)/part11.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_12of18.zip >$(SOURCE_DIR)/part12.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_13of18.zip >$(SOURCE_DIR)/part13.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_14of18.zip >$(SOURCE_DIR)/part14.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_15of18.zip >$(SOURCE_DIR)/part15.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_16of18.zip >$(SOURCE_DIR)/part16.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_16of18.zip >$(SOURCE_DIR)/part17.json
	unzip -p $(SOURCE_DIR)/psc-snapshot-*_16of18zip >$(SOURCE_DIR)/part18.json


load:
	$(PYTHON) read_psc.py



