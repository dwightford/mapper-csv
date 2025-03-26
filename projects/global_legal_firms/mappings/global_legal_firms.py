#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import hashlib

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data, input_row_num = None):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        json_data['DATA_SOURCE'] = '<supply>'

        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = '<remove_or_supply>'

        #--record type is not mandatory, but should be PERSON or ORGANIZATION
        #--json_data['RECORD_TYPE'] = 'PERSON'

        #--column mappings

        # columnName: SORT_ID
        # 100.0 populated, 100.0 unique
        #      SENDJ-1_00000001 (1)
        #      SENDJ-1_00000004 (1)
        #      SENDJ-1_00000005 (1)
        #      SENDJ-1_00000006 (1)
        #      SENDJ-1_00000007 (1)
        json_data['SORT_ID'] = raw_data['SORT_ID']

        # columnName: Company_Name
        # 100.0 populated, 99.08 unique
        #      K&L Gates Llp (2)
        #      Reed Smith Llp (2)
        #      Dechert Llp (2)
        #      Advokatfirmaet Thommessen As (2)
        #      Murgitroyd & Company Limited (2)
        json_data['Company_Name'] = raw_data['Company_Name']

        # columnName: Location_Type
        # 99.08 populated, 0.93 unique
        #      Headquarters (346)
        #      Principal Subsidiary (102)
        #      Subsidiary (83)
        #      Single Location (6)
        #      Branch (2)
        json_data['Location_Type'] = raw_data['Location_Type']

        # columnName: Ownership_Type
        # 100.0 populated, 0.37 unique
        #      Unlisted (532)
        #      Listed (12)
        json_data['Ownership_Type'] = raw_data['Ownership_Type']

        # columnName: Dow_Jones_Industry
        # 100.0 populated, 0.37 unique
        #      Legal Services (543)
        #      Advocacy Services (Discontinued from 15th March 2016) (1)
        json_data['Dow_Jones_Industry'] = raw_data['Dow_Jones_Industry']

        # columnName: SIC
        # 99.82 populated, 3.31 unique
        #      8111 (479)
        #      9222 (27)
        #      6541 (13)
        #      7389 (7)
        #      6719 (2)
        json_data['SIC'] = raw_data['SIC']

        # columnName: SIC_Descriptor
        # 99.82 populated, 3.31 unique
        #      Legal Services (479)
        #      Legal Counsel and Prosecution (27)
        #      Title Abstract Offices (13)
        #      Business Services, NEC (7)
        #      Offices of Holding Companies, NEC (2)
        json_data['SIC_Descriptor'] = raw_data['SIC_Descriptor']

        # columnName: NAICS
        # 100.0 populated, 3.49 unique
        #      541110 (480)
        #      922130 (27)
        #      541191 (13)
        #      541199 (4)
        #      561499 (3)
        json_data['NAICS'] = raw_data['NAICS']

        # columnName: NAICS_Descriptor
        # 100.0 populated, 3.49 unique
        #      Offices of Lawyers (480)
        #      Legal Counsel and Prosecution (27)
        #      Title Abstract and Settlement Offices (13)
        #      All Other Legal Services (4)
        #      All Other Business Support Services (3)
        json_data['NAICS_Descriptor'] = raw_data['NAICS_Descriptor']

        # columnName: NACE
        # 100.0 populated, 3.49 unique
        #      74.11 (494)
        #      75.23 (27)
        #      74.8 (4)
        #      74.87 (2)
        #      74.85 (2)
        json_data['NACE'] = raw_data['NACE']

        # columnName: NACE_Descriptor
        # 100.0 populated, 3.49 unique
        #      Legal activities (494)
        #      Justice and judicial activities (27)
        #      Miscellaneous business activities n.e.c. (4)
        #      Other business activities n.e.c. (2)
        #      Secretarial and translation activities (2)
        json_data['NACE_Descriptor'] = raw_data['NACE_Descriptor']

        # columnName: Exchange
        # 2.21 populated, 33.33 unique
        #      London Stock Exchange (5)
        #      Australian Stock Exchange Ltd (4)
        #      The Nasdaq Stock Market (2)
        #      Tokyo Stock Exchange (1)
        json_data['Exchange'] = raw_data['Exchange']

        # columnName: Index_Membership
        # 0.55 populated, 66.67 unique
        #      Dow Jones U.S. Total Market Index, NASDAQ, Russell 3000 Index (Nasdaq), Russell 2000 Index (Nasdaq) (2)
        #      ASX All Ordinaries (1)
        json_data['Index_Membership'] = raw_data['Index_Membership']

        # columnName: Auditor/Accountant
        # 8.64 populated, 76.6 unique
        #      RSM NORGE AS (7)
        #      RSM International (3)
        #      Deloitte Touche Tohmatsu Limited (2)
        #      CROWE PARTNER REVISJON AS (2)
        #      Gelman Rosenberg & Freedman B (2)
        json_data['Auditor/Accountant'] = raw_data['Auditor/Accountant']

        # columnName: URL
        # 90.81 populated, 86.64 unique
        #      http://www.dentons.com (6)
        #      http://www.dlapiper.com (5)
        #      http://www.cms.law (5)
        #      http://www.bakermckenzie.com (5)
        #      http://www.nortonrosefulbright.com (4)
        json_data['URL'] = raw_data['URL']

        # columnName: Address1
        # 95.22 populated, 96.33 unique
        #      'tower 2 Darling Park' Level 22 201 Sussex Street (4)
        #      4 More London Riverside (3)
        #      70 Sir John Rogerson's Quay (2)
        #      One Wood Street (2)
        #      One Fleet Place (2)
        json_data['Address1'] = raw_data['Address1']

        # columnName: Address2
        # 19.49 populated, 98.11 unique
        #      100 Old Hall Street (2)
        #      Auckland Central (2)
        #      Off Nana Mava Main Road (1)
        #      2n Philip Courtney  Canary Wharf (1)
        #      50 Holborn Viaduct (1)
        json_data['Address2'] = raw_data['Address2']

        # columnName: Contact_Name
        # 98.53 populated, 98.51 unique
        #      Andrew Nathaniel Blattman (4)
        #      Alexander Jozef Kaarls (3)
        #      Peter Clough (2)
        #      Elisabeth Antonia Maria Stanley-Smith-Van Der Velden (2)
        #      John Park (2)
        json_data['Contact_Name'] = raw_data['Contact_Name']

        # columnName: Contact_Title
        # 94.67 populated, 10.68 unique
        #      Director (96)
        #      Designated Limited Liability Partnership Member (91)
        #      Partner (54)
        #      President (34)
        #      Managing Partner (32)
        json_data['Contact_Title'] = raw_data['Contact_Title']

        # columnName: Contact_Number
        # 85.11 populated, 96.33 unique
        #      44 2072421212 (3)
        #      61 282478000 (3)
        #      44 2030880000 (2)
        #      44 2073490296 (2)
        #      44 2079194500 (2)
        json_data['Contact_Number'] = raw_data['Contact_Number']

        # columnName: Contact_Fax
        # 10.85 populated, 98.31 unique
        #      45 33344001 (2)
        #      52 5553461500 (1)
        #      34 913992408 (1)
        #      41 813004988 (1)
        #      69 6788204 (1)
        json_data['Contact_Fax'] = raw_data['Contact_Fax']

        # columnName: Contact_E-Mail_Address
        # 0.55 populated, 100.0 unique
        #      companysecretary@iphltd.com.au (1)
        #      nick@anexo-group.com (1)
        #      ir@nex-tone.co.jp (1)
        json_data['Contact_E-Mail_Address'] = raw_data['Contact_E-Mail_Address']

        # columnName: City
        # 100.0 populated, 41.54 unique
        #      London (77)
        #      Sydney (22)
        #      New York (19)
        #      Oslo (13)
        #      Toronto (12)
        json_data['City'] = raw_data['City']

        # columnName: State/Province
        # 96.88 populated, 31.12 unique
        #      London (75)
        #      NSW (26)
        #      CA (20)
        #      NY (19)
        #      ON (15)
        json_data['State/Province'] = raw_data['State/Province']

        # columnName: Country/Region
        # 100.0 populated, 6.8 unique
        #      USA (149)
        #      England (120)
        #      Australia (43)
        #      China (31)
        #      Canada (26)
        json_data['Country/Region'] = raw_data['Country/Region']

        # columnName: Postal_Code
        # 96.32 populated, 85.5 unique
        #      2000 (25)
        #      3000 (6)
        #      4000 (5)
        #      1010 (5)
        #      00130 (4)
        json_data['Postal_Code'] = raw_data['Postal_Code']

        # columnName: Phone
        # 86.58 populated, 96.39 unique
        #      44 2072421212 (3)
        #      61 282478000 (3)
        #      44 2030880000 (2)
        #      44 2073490296 (2)
        #      44 2079194500 (2)
        json_data['Phone'] = raw_data['Phone']

        # columnName: Fax
        # 11.58 populated, 98.41 unique
        #      45 33344001 (2)
        #      52 5553461500 (1)
        #      34 913992408 (1)
        #      41 813004988 (1)
        #      69 6788204 (1)
        json_data['Fax'] = raw_data['Fax']

        # columnName: Sales,_USD
        # 100.0 populated, 95.04 unique
        #      60552900.0 (10)
        #      72891993.0 (5)
        #      71136000.0 (4)
        #      143701914.0 (3)
        #      247051200.0 (2)
        json_data['Sales,_USD'] = raw_data['Sales,_USD']

        # columnName: Audit_Fees_(Including_Non-Audit_Fees),_USD
        # 2.21 populated, 100.0 unique
        #      4378000.0 (1)
        #      869372.0 (1)
        #      265422.0 (1)
        #      470177.0 (1)
        #      371591.0 (1)
        json_data['Audit_Fees_(Including_Non-Audit_Fees),_USD'] = raw_data['Audit_Fees_(Including_Non-Audit_Fees),_USD']

        # columnName: Employees
        # 97.79 populated, 69.55 unique
        #      9.0 (18)
        #      410.0 (13)
        #      2.0 (12)
        #      500.0 (7)
        #      1600.0 (6)
        json_data['Employees'] = raw_data['Employees']

        # columnName: Market_Cap,_USD
        # 2.02 populated, 100.0 unique
        #      1498368527.0 (1)
        #      850987530.0 (1)
        #      231203719.0 (1)
        #      95445651.0 (1)
        #      143714718.0 (1)
        json_data['Market_Cap,_USD'] = raw_data['Market_Cap,_USD']

        # columnName: Sales_Growth
        # 28.68 populated, 19.23 unique
        #      0.0 (127)
        #      0.0657248068079724 (1)
        #      0.254180775164901 (1)
        #      8.29244759781567 (1)
        #      0.0760697305863708 (1)
        json_data['Sales_Growth'] = raw_data['Sales_Growth']

        # columnName: Employee_Growth
        # 27.57 populated, 14.67 unique
        #      0.0 (128)
        #      0.02 (2)
        #      -0.18 (1)
        #      -0.139551699204628 (1)
        #      0.230769230769231 (1)
        json_data['Employee_Growth'] = raw_data['Employee_Growth']

        # columnName: EPS,_USD
        # 0.37 populated, 100.0 unique
        #      0.0717691536146902 (1)
        #      0.575770514546094 (1)
        json_data['EPS,_USD'] = raw_data['EPS,_USD']

        # columnName: Net_Income,_USD
        # 35.66 populated, 79.38 unique
        #      0.0 (40)
        #      5318392.0 (2)
        #      37838064000.0 (1)
        #      151955605.0 (1)
        #      974298031.0 (1)
        json_data['Net_Income,_USD'] = raw_data['Net_Income,_USD']

        # columnName: Net_Profit_Margin
        # 2.21 populated, 100.0 unique
        #      0.021117647682023 (1)
        #      0.100396301188904 (1)
        #      0.0584027085314102 (1)
        #      0.101256244391766 (1)
        #      0.0656654907740219 (1)
        json_data['Net_Profit_Margin'] = raw_data['Net_Profit_Margin']

        # columnName: Total_Assets,_USD
        # 41.91 populated, 96.93 unique
        #      0.0 (7)
        #      -9.22337203685478E+18 (2)
        #      331022739000.0 (1)
        #      1974471885.0 (1)
        #      2501947381.0 (1)
        json_data['Total_Assets,_USD'] = raw_data['Total_Assets,_USD']

        # columnName: Total_Liabilities,_USD
        # 36.95 populated, 97.01 unique
        #      0.0 (7)
        #      331022739000.0 (1)
        #      1974471885.0 (1)
        #      2501947381.0 (1)
        #      2557407423.0 (1)
        json_data['Total_Liabilities,_USD'] = raw_data['Total_Liabilities,_USD']

        # columnName: Beta,_last_5_years
        # 2.21 populated, 100.0 unique
        #      1.50293545185308 (1)
        #      0.703967698728634 (1)
        #      0.332445664157914 (1)
        #      0.299307150943388 (1)
        #      0.333443786419297 (1)
        json_data['Beta,_last_5_years'] = raw_data['Beta,_last_5_years']

        # columnName: P/E_Ratio
        # 1.84 populated, 100.0 unique
        #      157.382 (1)
        #      25.0 (1)
        #      15.5685 (1)
        #      5.30835 (1)
        #      11.2903 (1)
        json_data['P/E_Ratio'] = raw_data['P/E_Ratio']

        # columnName: Current_Ratio
        # 39.15 populated, 29.11 unique
        #      1.0 (17)
        #      1.4 (12)
        #      1.1 (10)
        #      2.8 (9)
        #      1.2 (9)
        json_data['Current_Ratio'] = raw_data['Current_Ratio']

        # columnName: Debt/Equity_Ratio
        # 2.02 populated, 100.0 unique
        #      5.35792553632562 (1)
        #      77.5342897682485 (1)
        #      51.5766018100111 (1)
        #      47.8306063281548 (1)
        #      80.9764612008777 (1)
        json_data['Debt/Equity_Ratio'] = raw_data['Debt/Equity_Ratio']

        # columnName: Debt/Assets_Ratio
        # 2.02 populated, 100.0 unique
        #      1.92902320542839 (1)
        #      37.4447997563575 (1)
        #      25.3995500272804 (1)
        #      29.5268209526976 (1)
        #      36.9397857877351 (1)
        json_data['Debt/Assets_Ratio'] = raw_data['Debt/Assets_Ratio']

        # columnName: Return_on_Equity
        # 2.21 populated, 100.0 unique
        #      0.0896696432301122 (1)
        #      0.100377571556166 (1)
        #      0.127162449587549 (1)
        #      0.0988152107853068 (1)
        #      0.102006039354214 (1)
        json_data['Return_on_Equity'] = raw_data['Return_on_Equity']

        # columnName: Return_on_Assets
        # 37.32 populated, 67.0 unique
        #      0.0 (23)
        #      0.008 (8)
        #      0.004 (6)
        #      0.052 (4)
        #      0.01 (3)
        json_data['Return_on_Assets'] = raw_data['Return_on_Assets']

        # columnName: Symbol
        # 2.02 populated, 100.0 unique
        #      LZ (1)
        #      jXIPH (1)
        #      lLGTLY (1)
        #      lLANX (1)
        #      lLKGH (1)
        json_data['Symbol'] = raw_data['Symbol']

        # columnName: DUNS
        # 100.0 populated, 100.0 unique
        #      985480701 (1)
        #      229501804 (1)
        #      202524930 (1)
        #      676294758 (1)
        #      671452274 (1)
        json_data['DUNS'] = raw_data['DUNS']

        # columnName: Business_Description
        # 97.98 populated, 7.32 unique
        #      Legal Services Office (445)
        #      Legal Counsel/Prosecution (27)
        #      Title Abstract Office (13)
        #      Legal Services Nsk (9)
        #      Business Services (3)
        json_data['Business_Description'] = raw_data['Business_Description']

        # columnName: Selected_Legal_Status_Categories
        # 93.57 populated, 1.38 unique
        #      Corporation (217)
        #      Limited Liability Partnership (198)
        #      Partnership of Unknown Type (39)
        #      State-Owned Company (26)
        #      Non-Profit Organization (15)
        json_data['Selected_Legal_Status_Categories'] = raw_data['Selected_Legal_Status_Categories']

        # columnName: Hierarchy_Designation
        # 58.09 populated, 0.63 unique
        #      Global Ultimate (281)
        #      Domestic Ultimate (35)
        json_data['Hierarchy_Designation'] = raw_data['Hierarchy_Designation']

        # columnName: Ultimate_Parent_Name
        # 100.0 populated, 85.48 unique
        #      IPH Ltd. (11)
        #      Government Of Socialist Republic Of Vietnam (11)
        #      Government Of The Republic Of China (Taiwan) (5)
        #      Allen Overy Shearman Sterling Llp (4)
        #      Gobierno Federal De Los Estados Unidos Mexicanos (4)
        json_data['Ultimate_Parent_Name'] = raw_data['Ultimate_Parent_Name']

        # columnName: Ultimate_Parent_Location
        # 100.0 populated, 7.54 unique
        #      USA (149)
        #      England (125)
        #      Australia (38)
        #      China (31)
        #      Canada (26)
        json_data['Ultimate_Parent_Location'] = raw_data['Ultimate_Parent_Location']

        #--remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #-----------------------------------
    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:           
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    #----------------------------------------
    def format_date(self, raw_date):
        try: 
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except: 
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    #----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'input/global_legal_firms.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n') 
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    output_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        input_row_count += 1

        json_data = mapper.map(input_row, input_row_count)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

