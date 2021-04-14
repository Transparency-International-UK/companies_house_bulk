#!/usr/bin/env python

import glob
from multiprocessing.pool import ThreadPool

from countrynames import to_code
from fingerprints import generate

import dataset
from dataset.chunked import ChunkedInsert

# customise the DB_URI
DB_URI = ''
DATADIR = 'data'

# connect to public schema
db = dataset.connect(DB_URI)

appointments_table = db['appointments_all']
companies_table = db['companies_with_appointments']


def determine_line_type(line):
    if (line.startswith('DDDD')      # first line
        or line.strip().isdigit()):  # last line
        return 'header'

    elif line[8] == '1':  # company record
        return 'company'

    elif line[8] == '2':  # person (natural or legal)
        return 'officer'

    else:  # can't identify its type. The line is probably broken.
        with open("broken_lines.txt", "a") as fh:
            fh.write(line + "\n")


def process_directory(dirpath):
    pattern = f'{dirpath}/Prod195*.dat'
    tp = ThreadPool(10)

    # check we have all files we want to process.
    # Then map process_file to pattern.

    for filepath in glob.glob(pattern):
        print(filepath)

    tp.map(process_file, glob.glob(pattern))
    tp.close()
    tp.join()


def process_file(filepath):

    personstorer = ChunkedInsert(appointments_table)
    companystorer = ChunkedInsert(companies_table)

    # There is a 0x85 in string line that is interpreted as an ellipsis
    # character and breaks way too many lines.  I suspect that the
    # cp1252-encoded file was run through a latin1-to-utf8 conversion
    # process in CH.  That is, in true cp1252 there was a byte 0x85, then
    # after the latin1-to-utf8 process there was the two-byte sequence
    # b'\xc2\x85', which gets interpreted as the character U+0085
    # (ellipsis, '...').

    with open(filepath, encoding='utf-8') as fh:

        for num, line in enumerate(fh):

            byteline = bytearray(line, encoding='utf-8')\
                                .replace(b'\xc2\x85', b'')
            # decode returns a string
            line_clean = byteline.decode(encoding='utf-8')

            linetype = determine_line_type(line_clean)

            if linetype == 'header':
                continue

            elif linetype == 'company':
                current_company = strip_junk(parse_company(line_clean))
                companystorer.insert(current_company)

            elif linetype == 'officer':
                officer = strip_junk(parse_officer(line_clean))
                personstorer.insert(officer)

            else:
                print(f"Line #{num} in {filepath} could not be parsed.\n")

    with companystorer:
        companystorer.flush()

    with personstorer:
        personstorer.flush()


def strip_junk(d):
    return {k: v for k, v in d.items()
            if not (k.startswith('mystery')
                    or k.startswith('unwanted')
                    or k.startswith('filler'))}


def parse_officer(line):

    results = dict()

    # number of the company to which this officer is appointed to.
    # The majority of company numbers are 8 digit numeric; however, some
    # consist of a prefix of 2 alphanum characters
    # followed by 6 digits.
    results['appointed_to_company_number'] = line[0:8]

    # nature of the officer.
    # 1 is person (as in officer, could be legal or natural person)
    # 2 is company (as in companies to which the officer is appointed to)
    results['record_type'] = line[8]

    # source document of the appointment date.
    results['appointment_date_origin_code'] = line[9:10]

    # role of the appointed officer.
    results['officer_role_code'] = line[10:12]

    # personal number: as of 2009 pnr are composed of 12 digits. The first
    # 8 uniquely identify the person.  A person is composed of a name and a
    # usual residence address (URA) which is *not* public. If director with
    # many appointments changes URA or name for an appointment then the pnr
    # last 4 digits will be incremented from 0000 to 0001.
    results['pnr'] = line[12:24]

    # indicator for record being a company.  officer can be either natual
    # (homo sapiens =! Y) or legal (corporation == Y) person.
    results['is_company'] = line[24] == 'Y'

    # filler, can throw away.
    results['filler_a'] = line[25:32]

    # appointment dates.  If a date is provided for officer_role_code 11,
    # 12, or 13 this refers to the date that the form was registered.
    # Resigned appointments are not normally included in a snapshot so this
    # field will usually be blank.  date format: CCYYMMDD (C for century, Y
    # for year, M for month, D for day).
    results['start_date_text'] = line[32:40]
    results['end_date_text'] = line[40:48]

    # postal code.
    results['service_address_post_code'] = line[48:56]

    # dob.  partial_dob field will contain either all spaces, or a partial
    # dob followed by 2 space chars ‘CCYYMM ‘.  If full_dob is provided
    # then partial_dob will also be provided, but partial_dob may be
    # provided w/out full_dob.
    results['partial_dob'] = line[56:64]

    # full_dob could be thrown away but we keep for completeness.
    # tested on 1000 records, always '        '.
    results['full_dob'] = line[64:72]

    # holds the length of the variable data bit (incl. "<" chars), used for
    # validation, do not insert in database.
    results['unwanted_variable_data_length'] = line[72:76]

    # variable_data: contains officer’s name, service address, occupation,
    # and nationality, formatted as below:
    # TITLE                      |-> 'title'
    # <FORENAMES                 |-> 'name'
    # <SURNAME                   |-> 'surname'
    # <HONOURS                   |-> 'honours'
    # <CARE OF                   |-> 'service_address_care_of'
    # <PO BOX                    |-> 'service_address_po_box'
    # <ADDRESS LINE 1            |-> 'service_address_line_1'
    # <ADDRESS LINE 2            |-> 'service_address_line_2'
    # <POST TOWN                 |-> 'service_address_post_town'
    # <COUNTY                    |-> 'service_address_county'
    # <COUNTRY                   |-> 'service_address_country'
    # <OCCUPATION                |-> 'occupation'
    # <NATIONALITY               |-> 'nationality'
    # <USUAL RESIDENTIAL COUNTRY |-> 'ura_country'
    # <                          |-> 'filler_b'

    # Each variable data field will contain 14 “<” delimiters.  Consecutive
    # “<” delimiters indicates that the particular element of the variable
    # data is not present.

    variable_data = line[76:].rstrip(' \n')
    vardata = variable_data.split('<')
    vardata_components = (
            'title',
            'name',
            'surname',
            'honours',
            'service_address_care_of',
            'service_address_po_box',
            'service_address_line_1',
            'service_address_line_2',
            'service_address_post_town',
            'service_address_county',
            'service_address_country',
            'occupation',
            'nationality',
            'ura_country',
            'filler_b',)  # after the last '<' there's just a bunch of
                          # white spaces till end of line, can throw away.

    for component, datapoint in zip(vardata_components, vardata):
        results[component] = datapoint

        results["ura_country_norm"] = to_code(
            results.get("ura_country_norm", None), fuzzy=True)
        results["nationality_norm"] = to_code(
            results.get("nationality", None), fuzzy=True)
        results["service_address_country_norm"] = to_code(
            results.get("service_address_country", None), fuzzy=True)

        results["name_fp"] = generate(results.get("name", "") + " " +
                                      results.get("surname", ""))

    return results


def parse_company(line):

        results = dict()

        # same nomenclature for company_number in function parse_officer()
        # applies.
        results['company_number'] = line[0:8]

        # record_type is always 1 since we're parsing companies.
        results['record_type'] = line[8]

        # company_status (dissolved, active...)
        results['company_status_code'] = line[9]

        results['is_company'] = line[24] == 'Y'

        # filler, can throw away.
        results['filler'] = line[10:32]

        results['number_of_officers'] = line[32:36]

        # holds the length of the name variable (incl. "<" char), used for
        # validation, do not insert in database.
        results['unwanted_company_name_length'] = line[36:40]

        # company names will be of varying length and will always end with
        # '...< \n'.
        results['company_name'] = line[40:].strip('< \n')
        results["company_name_norm"] = generate(results.get("company_name",
                                                            None))
        return results


if __name__ == '__main__':
    process_directory(DATADIR)

