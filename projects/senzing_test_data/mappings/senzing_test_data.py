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
        json_data2 = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        json_data['DATA_SOURCE'] = 'SENZING_TEST_COMPANY'
        json_data['RECORD_TYPE'] = 'ORGANIZATION'

        json_data2['DATA_SOURCE'] = 'SENZING_TEST_CONTACT'
        json_data2['RECORD_TYPE'] = 'PERSON'




        #--the record_id should be unique, remove this mapping if there is not one 

        #--record type is not mandatory, but should be PERSON or ORGANIZATION
        #--json_data['RECORD_TYPE'] = 'PERSON'

        #--column mappings

        # columnName: SORT_ID
        # 100.0 populated, 100.0 unique
        #      SENDJ-2_0000001 (1)
        #      SENDJ-2_0000002 (1)
        #      SENDJ-2_0000003 (1)
        #      SENDJ-2_0000004 (1)
        #      SENDJ-2_0000005 (1)
        json_data['RECORD_ID'] = raw_data['SORT_ID']
        json_data2['RECORD_ID'] = raw_data['SORT_ID']


        # columnName: CompanyName
        # 100.0 populated, 99.32 unique
        #      Bnp Paribas (17)
        #      Tata Consultancy Services Limited (10)
        #      Phillips 66 (10)
        #      Mitsubishi Corporation (7)
        #      Thai Airways International Public Company Limited (6)
        json_data['NAME_ORG'] = raw_data['CompanyName']

        # columnName: BusinessDescription
        # 97.75 populated, 18.22 unique
        #      Holding Company (705)
        #      Business Services (568)
        #      <businessDescription /> (543)
        #      General Government (361)
        #      Whol Nondurable Goods (300)
        # json_data['BusinessDescription'] = raw_data['BusinessDescription']

        # columnName: DUNS
        # 100.0 populated, 100.0 unique
        #      643409170 (1)
        #      534792325 (1)
        #      559235592 (1)
        #      535063291 (1)
        #      552281008 (1)
        json_data['DUNS_NUMBER'] = raw_data['DUNS']

        # columnName: Sales
        # 84.07 populated, 81.55 unique
        #      0 (200)
        #      29669400 (195)
        #      1136086 (126)
        #      4276320000 (88)
        #      1200760 (66)
        #json_data['Sales'] = raw_data['Sales']

        # columnName: Employees
        # 81.43 populated, 20.25 unique
        #      10 (935)
        #      5 (760)
        #      3 (671)
        #      2 (652)
        #      7 (562)
       # json_data['Employees'] = raw_data['Employees']

        # columnName: Industry
        # 97.48 populated, 4.35 unique
        #      i61 (1252)
        #      ibcs (807)
        #      i8396 (670)
        #      i5010022 (524)
        #      ifodwh (485)
        # json_data['Industry'] = raw_data['Industry']

        # columnName: SIC
        # 98.88 populated, 4.01 unique
        #      6719 (785)
        #      7389 (645)
        #      9199 (368)
        #      8711 (321)
        #      5199 (320)
        # json_data['SIC'] = raw_data['SIC']

        # columnName: NACE
        # 97.63 populated, 2.15 unique
        #      45.21 (861)
        #      74.87 (745)
        #      74.15 (588)
        #      51.56 (549)
        #      51.87 (499)
        # json_data['NACE'] = raw_data['NACE']

        # columnName: NAICS
        # 99.48 populated, 4.29 unique
        #      551112 (805)
        #      561499 (616)
        #      522110 (402)
        #      921190 (367)
        #      424990 (341)
        #json_data['NAICS'] = raw_data['NAICS']

        # columnName: Address1
        # 98.26 populated, 92.34 unique
        #      tower' B 7 Westbourne Street (54)
        #      Main Street (51)
        #      Damascus (43)
        #      1 Woolworths Way (37)
        #      L 8 154 Pacific Hwy (32)

        json_data['ADDR_TYPE'] = 'BUSINESS'

        json_data['ADDR_LINE1'] = raw_data['Address1']

        # columnName: Address2
        # 29.3 populated, 79.11 unique
        #      Auckland Central (72)
        #      Centro (32)
        #      Chuo-Ku (26)
        #      Wellington Central (26)
        #      Newmarket (25)
        json_data['ADDR_LINE2'] = raw_data['Address2']

        # columnName: Address3
        # 3.03 populated, 64.86 unique
        #      Chiyoda-Ku (35)
        #      Minato-Ku (25)
        #      Gangnam-gu (16)
        #      Chuo-Ku (16)
        #      Jung-gu (15)
        json_data['ADDR_LINE3'] = raw_data['Address3']

        # columnName: City
        # 97.99 populated, 35.69 unique
        #      Singapore (577)
        #      London (483)
        #      Auckland (426)
        #      Mexico (333)
        #      Dubai (287)
        json_data['ADDR_CITY'] = raw_data['City']

        # columnName: State
        # 88.22 populated, 6.96 unique
        #      NSW (484)
        #      London (426)
        #      AUK (368)
        #      CMX (335)
        #      HK (312)
        json_data['ADDR_STATE'] = raw_data['State']

        # columnName: Country
        # 99.97 populated, 0.67 unique
        #      USA (1800)
        #      England (1411)
        #      Australia (1166)
        #      China (1118)
        #      Mexico (975)
        json_data['ADDR_COUNTRY'] = raw_data['Country']

        # columnName: Region
        # 100.0 populated, 0.6 unique
        #      USA (2694)
        #      UK (1772)
        #      AUSTR (1166)
        #      CHINA (1124)
        #      GFR (1102)
        # json_data['Region'] = raw_data['Region']

        # columnName: PostalCode
        # 82.99 populated, 66.52 unique
        #      2000 (170)
        #      1010 (155)
        #      2065 (97)
        #      6000 (84)
        #      6011 (76)
        json_data['ADDR_POSTAL_CODE'] = raw_data['PostalCode']

        # columnName: AreaCode
        # 10.64 populated, 11.17 unique
        #      212 (92)
        #      416 (73)
        #      905 (51)
        #      631 (51)
        #      800 (49)
        #json_data['PHONE_AREA_CODE'] = raw_data['AreaCode']

        # columnName: Phone
        # 73.2 populated, 94.59 unique
        #      61 294333444 (69)
        #      61 288850000 (32)
        #      61 282476300 (29)
        #      61 893274211 (25)
        #      61 385303500 (24)
   

        json_data['PHONE_NUMBER'] = str(raw_data['AreaCode']) + str(raw_data['Phone'])

        # columnName: Fax
        # 23.69 populated, 98.33 unique
        #      7 () (20)
        #      65 67132999 (5)
        #      27 117885092 (3)
        #      65 65706056 (3)
        #      34 915550132 (3)
        json_data['PHONE_TYPE'] = raw_data['Fax']

        # columnName: URL
        # 56.53 populated, 85.31 unique
        #      http://www.bp.com (39)
        #      http://www.glencore.com.au (37)
        #      http://www.ramsayhealth.com.au (37)
        #      http://www.woolworthsgroup.com.au (29)
        #      http://www.bhp.com (28)
        json_data['WEBSITE_ADDRESS'] = raw_data['URL']

        # columnName: MainContactName
        # 95.59 populated, 95.3 unique
        #      Michael Stanley Siddle (105)
        #      Sophie Alexandra Moore (36)
        #      Anthony Natale Gianotti (29)
        #      Frank Gerard Calabria (29)
        #      Ian Cribb (20)
        json_data2['NAME_FULL'] = raw_data['MainContactName']

        # columnName: MainContactJobTitle
        # 84.49 populated, 1.86 unique
        #      Director (5965)
        #      President (2814)
        #      Proprietor (1327)
        #      Managing Director (1025)
        #      Manager (844)
        # json_data2['MainContactJobTitle'] = raw_data['MainContactJobTitle']

        # columnName: MainContactPhone
        # 2.95 populated, 98.78 unique
        #      1 416 (3)
        #      86 10 (2)
        #      1 212 5485544 (2)
        #      1 952 8284144 (2)
        #      65 68784751 (2)
        json_data2['PHONE_NUMBER'] = raw_data['MainContactPhone']

        # columnName: MainContactEmail
        # 6.44 populated, 99.5 unique
        #      IR@pingan.com.cn (2)
        #      andrew_slabin@discovery.com (2)
        #      investor.relations@iairgroup.com (2)
        #      investor@ferguson.com (2)
        #      invrel@pge-corp.com (2)
        json_data2['EMAIL_ADDRESS'] = raw_data['MainContactEmail']

        #--remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        json_data2 = self.remove_empty_tags(json_data2)
        self.capture_mapped_stats(json_data2)

        return json_data, json_data2

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

#----------------------------------------output_file_handle2
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'input/senzing_test_data.csv'
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

        json_data,json_data2 = mapper.map(input_row, input_row_count)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if json_data2:
            output_file_handle.write(json.dumps(json_data2) + '\n')
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

