SOURCE_DIR = data
SHELL = /usr/bin/env bash -o pipefail  # if a command fails, the pipeline of commands will fail.
CONFILE = database

include $(CONFILE).mk

PSQL = /usr/bin/psql "postgresql://$(user):$(password)@localhost:5432/$(database)"

all: clean download unpack load

test-parameters:
	@echo "pg user is: $(user)"
	@echo "database name is: $(database)"
	@echo "passsword to connect to pg is: $(password)"
	@echo "source directory is: $(SOURCE_DIR)"

clean:
	rm -rf $(SOURCE_DIR)/*.zip
	rm -rf $(SOURCE_DIR)/*.csv

download:
	mkdir -p $(SOURCE_DIR)
	wget -r -nH -c -np -l 1 -A zip --accept-regex "part[1-6]_6.zip" -P $(SOURCE_DIR) http://download.companieshouse.gov.uk/en_output.html

unpack:
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part1_6.zip >$(SOURCE_DIR)/part1.csv
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part2_6.zip >$(SOURCE_DIR)/part2.csv
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part3_6.zip >$(SOURCE_DIR)/part3.csv
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part4_6.zip >$(SOURCE_DIR)/part4.csv
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part5_6.zip >$(SOURCE_DIR)/part5.csv
	unzip -p $(SOURCE_DIR)/BasicCompanyData-*-part6_6.zip >$(SOURCE_DIR)/part6.csv


load:
	$(PSQL) -f profiles_schema.sql
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part1.csv
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part2.csv
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part3.csv
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part4.csv
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part5.csv
	$(PSQL) -c "COPY bulk_companyprofiles FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'utf-8'" <$(SOURCE_DIR)/part6.csv


