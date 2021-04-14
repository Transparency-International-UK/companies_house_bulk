#!/usr/bin/env python3

import dataset
from countrynames import to_code
from fingerprints import generate
import glob

from multiprocessing.pool import ThreadPool
import json
from flatten import flatten_nested_dicts_only as flatten

DB_URI = ''  # customise
DATADIR = 'data'  # make sure this is the same here and in the Makefile!


# connect to public schema
db = dataset.connect(DB_URI)

psc_table = db['bulk_psc']
address_table = db['bulk_psc_address']
control_table = db['bulk_psc_natures_of_control']
identification_table = db['bulk_psc_identification']
exemptions_table = db['bulk_psc_exemptions']
summary_table = db['bulk_psc_summary']


def process_directory(dirpath):
	pattern = f'{dirpath}/part*.json'
	tp = ThreadPool(10)
	# check we have all files we want to process.
	# Then map process_file to pattern.
	for filepath in glob.glob(pattern):
		print(filepath)
	tp.map(process_file, glob.glob(pattern))
	tp.close()
	tp.join()


def process_file(filepath):

	with open(filepath) as f:
		for ix, line in enumerate(f):

			print(f"Inserting line {ix} of file {filepath}")

			# check line is not empty string.
			if line.strip():

				jsonline = json.loads(line)
				line_type = determine_line(jsonline)

				if line_type == "psc":

					(current_psc,
					 current_address,
					 current_identification,
					 current_control) = unpack_psc_line(jsonline)

					# normalise some fields and insert
					current_psc["name_fp"] = generate(current_psc["name"])
					current_psc["country_of_residence_norm"] = to_code(
						current_psc.get("country_of_residence", None),
						fuzzy=True)
					current_psc["nationality_norm"] = to_code(
						current_psc.get("nationality", None),
						fuzzy=True)
					psc_id = psc_table.insert(current_psc)

					if current_address:

						# normalise country field and insert
						current_address["country_norm"] = to_code(
							current_address.get("country", None),
							fuzzy=True)
						address_table.insert({**current_address,
											  **{"psc_serial_id": psc_id}})
					if current_identification:
						current_identification["country_registered_norm"] = to_code(
							current_identification.get("country_registered", None),
							fuzzy=True)

						identification_table.insert({**current_identification,
													 **{"psc_serial_id": psc_id}})
					if current_control:

						# stack the array of control types like this

						# company_number | nature_of_control
						# -----------------------------------
						# OC123456       | sometypeofcotrol_1
						# OC123456       | sometypeofcotrol_2
						# OC123456       | sometypeofcotrol_3

						root = current_control["company_number"]

						for nature in current_control["natures_of_control"]:
							stacked_control_data = {"company_number": root,
													"psc_serial_id": psc_id,
													"nature_of_control": nature}
							control_table.insert(stacked_control_data)

				# the exempted psc json is different. Needs its own processing.

				elif line_type == "exemptions":

					# lazyly create the list of dictionaries to be inserted as
					# records into the table.
					exemptions_generator = unpack_exemptions_line(
						json.loads(line))

					for exemption_dict in list(exemptions_generator):
						exemptions_table.insert(exemption_dict)

				elif line_type == "summary_line":

					# example of summary_line below:

					# {"data":
					#   {
					# "kind": "totals#persons-of-significant-control-snapshot",
					# "persons_of_significant_control_count": 7131880,
					# "statements_count": 564130,
					# "exemptions_count": 92,
					# "generated_at"    : "2020-03-25T03:39:38Z"}
					# }

					summary_data = jsonline.pop("data")
					summary_table.insert(summary_data)

			# if line is empty, go to next one.
			else:
				continue


def determine_line(jsonline):

	if "company_number" in jsonline and "data" in jsonline:

		if jsonline["data"]["kind"] == "exemptions":
			return "exemptions"

		else:
			return "psc"

	elif "company_number" not in jsonline and "data" in jsonline:
		return "summary_line"


def unpack_psc_line(jsonline):

	root = jsonline.pop("company_number")
	data = jsonline.pop("data")
	control = data.pop("natures_of_control", None)
	address = data.pop("address", None)
	identification = data.pop("identification", None)
	flat_data = {"company_number": root, **flatten(data)}

	# add root company number to the branches of the tree.
	address_dic = {"company_number": root, **address} if (address is not
														  None) else address
	identification_dic = {"company_number": root, **identification} if (
			identification is not None) else identification
	control_dic = {"natures_of_control": control, "company_number": root} if (
			control is not None) else control

	return flat_data, address_dic, identification_dic, control_dic


def unpack_exemptions_line(jsonline):

	# we are aiming to build the following table.

	# etag | company_number | exemption_type | from    | to      | kind       | links_self
	# ---------------------------------------------------------------------------------------
	# 1we4 | OC123456       | type_1         | 03.2009 | 06.2010 | exemptions | resource_link
	# 1we4 | OC123456       | type_2         | 01.2010 | 05.2012 | exemptions | resource_link
	# 1we4 | OC123456       | type_2         | 04.2013 | 02.2015 | exemptions | resource_link
	# 34rf | OC456789       | type_3         | 07.2008 | 05.2009 | exemptions | resource_link

	root = jsonline.pop("company_number")
	data = jsonline.pop("data")
	exemptions = data.pop("exemptions")

	for k, v in exemptions.items():

		type_ = v["exemption_type"]

		for date_dic in v["items"]:

			# the generator will contain dictionaries which will map to
			# a pg line like above.

			yield {"company_number": root, **flatten(data),
				   "exemption_type": type_, **date_dic}


if __name__ == '__main__':
	process_directory(DATADIR)
