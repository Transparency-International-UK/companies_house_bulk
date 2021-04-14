DROP TABLE IF EXISTS bulk_companyprofiles;

CREATE TABLE bulk_companyprofiles (
    company_name TEXT,
    company_number TEXT,
    care_of TEXT,
    po_box TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    locality TEXT,
    region TEXT,
    country TEXT,
    postal_code TEXT,
    type TEXT,
    company_status TEXT,
    country_of_origin TEXT,
    date_of_cessation TEXT,
    date_of_creation TEXT,
    accounting_reference_date_day TEXT,
    accounting_reference_date_month TEXT,
    next_due TEXT,
    last_accounts_made_up_to TEXT,
    last_accounts_type TEXT,
    return_next_due TEXT,
    returns_last_made_up_to TEXT,
    number_mortgage_charges TEXT,
    number_outstanding_mortgages TEXT,
    number_part_satisfied_mortgages TEXT,
    number_satisfied_mortgages TEXT,
    sic_code_1 TEXT,
    sic_code_2 TEXT,
    sic_code_3 TEXT,
    sic_code_4 TEXT,
    lp_number_general_partners TEXT,
    lp_number_limited_partners TEXT,
    uri TEXT,
    previous_company_name_1_date TEXT,
    previous_company_name_1 TEXT,
    previous_company_name_2_date TEXT,
    previous_company_name_2 TEXT,
    previous_company_name_3_date TEXT,
    previous_company_name_3 TEXT,
    previous_company_name_4_date TEXT,
    previous_company_name_4 TEXT,
    previous_company_name_5_date TEXT,
    previous_company_name_5 TEXT,
    previous_company_name_6_date TEXT,
    previous_company_name_6 TEXT,
    previous_company_name_7_date TEXT,
    previous_company_name_7 TEXT,
    previous_company_name_8_date TEXT,
    previous_company_name_8 TEXT,
    previous_company_name_9_date TEXT,
    previous_company_name_9 TEXT,
    previous_company_name_10_date TEXT,
    previous_company_name_10 TEXT,
    confirmation_statement_next_due TEXT,
    confirmation_statement_next_made_up_to TEXT);

CREATE INDEX bulk_companyprofiles_num ON bulk_companyprofiles (company_number);
