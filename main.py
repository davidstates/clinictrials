from utilityfunctions import *

__author__ = 'David States'

import xml.etree.ElementTree as ET
import psycopg2.extensions
import os
import re

#
# Data obtained from ClinicalTrials.gov using URLs of the form
# https://clinicaltrials.gov/ct2/download_studies?term=lymphoma
# which returns a zip file of files, each an XML file on one study
#
# The program traverses a directory tree and tries to load all
# of the XML files that it finds
#

# Open the database connection
#db = postgresql.open()
con = None
try:
    con = psycopg2.connect("dbname=affigen user=dbuser host=affigenresearch0.ccq2bhqzswxn.us-east-1.rds.amazonaws.com password=affigen")
except:
    print("Unable to connect to the database")
    exit()

# Create a cursor for the database connection
try:
    cur = con.cursor(cursor_factory=MyCursor)
except:
    print("Unable to create cursor")
    con.close()
    exit()

# Set lit as the default schema
sql = 'SET search_path TO clin, public'
cur.executesql(sql)

# Get file list

filepattern = re.compile('.xml$')
#datadir = '/Users/david/Dropbox (Personal)/Data/ClinicalTrials/'
#datadir = '/Users/david/data/clinicaltrials/'
datadir = '/home/ec2-user/data/clinicaltrials/'
for path, dirs, files in os.walk(datadir):
    for fnm in files:
        m = filepattern.search(fnm)
        if m:
            filename = os.path.join(path, fnm)

            # Parse the XML data and get the nct_id trial identifier
            root = ET.parse(filename)
            nd = root.find('./id_info/nct_id')
            nct_id = nd.text

            # See if we already have this trial in the database
            sql = "select max(nct_id) from study where nct_id='" + nct_id + "'"
            qret = cur.executefetchone(sql)
            if qret[0] is None:
                print('Loading '+filename+' NCTID: '+nct_id)
            else:
                print('Already loaded:' + nct_id)
                continue

            download_date = getXmlText(root, './required_header/download_date')
            brief_title = getXmlText(root, './brief_title')
            official_title = getXmlText(root, './official_title')
            overall_status = getXmlText(root, './overall_status')
            study_type = getXmlText(root, './study_type')
            study_design = getXmlText(root, './study_design')
            start_date = getXmlText(root, './start_date')
            completion_date = getXmlText(root, './completion_date')
            why_stopped = getXmlText(root, './why_stopped')
            enrollment = getXmlText(root, './enrollment')
            if enrollment == '':
                enrollment="Null"
            number_of_arms = getXmlText(root, './number_of_arms')
            if number_of_arms == '':
                number_of_arms="Null"
            number_of_groups = getXmlText(root, './number_of_groups')
            if number_of_groups == "":
                number_of_groups="Null"
            phase = getXmlText(root, './phase')
            is_fda_regulated = getXmlText(root, './is_fda_regulated')
            has_expanded_access = getXmlText(root, './has_expanded_access')
            brief_summary = getXmlText(root, './brief_summary/textblock')
            detailed_description = getXmlText(root, './detailed_description/textblock')
            eligibility = getXmlText(root, './eligibility/criteria/textblock')

            sql = """
INSERT INTO study(nct_id, download_date, brief_title, official_title, overall_status, study_type, study_design, start_date, completion_date, why_stopped, phase, is_fda_regulated, has_expanded_access, brief_summary, detailed_description, eligibility, enrollment, number_of_arms, number_of_groups)
VALUES('{0}', '{1}', '{2}','{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}',{16},{17},{18});
""".format(nct_id, download_date, brief_title, official_title, overall_status,  study_type, study_design, start_date, completion_date, why_stopped, phase, is_fda_regulated, has_expanded_access, brief_summary, detailed_description, eligibility, enrollment, number_of_arms, number_of_groups)
            cur.executesql(sql)
            qret = cur.executefetchone("select id from study where nct_id='"+nct_id+"'")
            studyid = str(qret[0])
            print("NCTID:" + nct_id + " loading as study "+ studyid)

            identifier = getXmlText(root,'./id_info/org_study_id')
            sql = "INSERT INTO StudyIdentifier(studyid, id_type, identifier) VALUES('{0}', 'Orignal', '{1}');".format(studyid, identifier)
            cur.executesql(sql)

            for nd in root.findall('./id_info/secondary_id'):
                if nd is not None:
                    identifier = getXmlText(nd,'.')
                    sql = "INSERT INTO StudyIdentifier(studyid, id_type, identifier) VALUES('{0}', 'Secondary', '{1}');".format(studyid, identifier)
                    cur.executesql(sql)

            for nd in root.findall('./condition'):
                if nd is not None:
                    condition = nd.find('.').text.replace("'","")
                    sql = "INSERT INTO Condition(studyid, condition) VALUES('{0}', '{1}');".format(studyid, condition)
                    cur.executesql(sql)

            for nd in root.findall('./keyword'):
                if nd is not None:
                    keyword = nd.find('.').text.replace("'","")
                    sql = "INSERT INTO Keyword(studyid, keyword) VALUES('{0}', '{1}');".format(studyid, keyword)
                    cur.executesql(sql)

            for nd in root.findall('./intervention'):
                if nd is not None:
                    intervention_type = nd.find('./intervention_type').text.replace("'","")
                    intervention_name = nd.find('./intervention_name').text.replace("'","")
                    sql = "INSERT INTO Intervention(studyid, intervention_type, intervention_name) VALUES('{0}', '{1}', '{2}');".format(studyid, intervention_type, intervention_name)
                    cur.executesql(sql)

            for nd in root.findall('./primary_outcome'):
                if nd is not None:
                    measure = getXmlText(nd, './measure')
                    time_frame = getXmlText(nd, './time_frame')
                    safety_issue = getXmlText(nd, './safety_issue')
                    description = getXmlText(nd, './description')
                    sql = "INSERT INTO Outcome(studyid, outcome_type, measure, time_frame, safety_issue, description) VALUES('{0}', 'Primary', '{1}', '{2}', '{3}', '{4}');".format(studyid, measure, time_frame, safety_issue, description)
                    cur.executesql(sql)

            for nd in root.findall('./secondary_outcome'):
                if nd is not None:
                    measure = getXmlText(nd, './measure')
                    time_frame = getXmlText(nd, './time_frame')
                    safety_issue = getXmlText(nd, './safety_issue')
                    description = getXmlText(nd, './description')
                    sql = "INSERT INTO Outcome(studyid, outcome_type, measure, time_frame, safety_issue, description) VALUES('{0}', 'Secondary', '{1}', '{2}', '{3}', '{4}');".format(studyid, measure, time_frame, safety_issue, description)
                    cur.executesql(sql)

            for nd in root.findall('./arm_group'):
                if nd is not None:
                    arm_group_label = getXmlText(nd, './arm_group_label')
                    arm_group_type = getXmlText(nd, './arm_group_type')
                    description = getXmlText(nd, './description')
                    sql = "INSERT INTO ArmGroup(studyid, group_type, group_label, description) VALUES('{0}', '{1}', '{2}', '{3}');".format(studyid, arm_group_type, arm_group_label, description)
                    cur.executesql(sql)

            for nd in root.findall('./condition_browse/mesh_term'):
                if nd is not None:
                    term = nd.text.replace("'", "")
                    sql = "INSERT INTO MeshCondition(studyid, term) VALUES('{0}', '{1}');".format(studyid, term)
                    cur.executesql(sql)

            for nd in root.findall('./intervention_browse/mesh_term'):
                if nd is not None:
                    term = nd.text.replace("'", "")
                    sql = "INSERT INTO MeshIntervention(studyid, term) VALUES('{0}', '{1}');".format(studyid, term)
                    cur.executesql(sql)

            for nd in root.findall('./reference'):
                if nd is not None:
                    citation = getXmlText(nd, './citation')
                    pmid = getXmlText(nd, './PMID')
                    if pmid=='':
                        pmid='NULL'
                    sql = "INSERT INTO Citation(studyid, pmid, citation, results) VALUES('{0}', {1}, '{2}', '0');".format(studyid, pmid, citation)
                    cur.executesql(sql)

            for nd in root.findall('./results_reference'):
                if nd is not None:
                    citation = getXmlText(nd, './citation')
                    pmid = getXmlText(nd, './PMID')
                    if pmid=='':
                        pmid='NULL'
                    sql = "INSERT INTO Citation(studyid, pmid, citation, results) VALUES('{0}', {1}, '{2}', '1');".format(studyid, pmid, citation)
                    cur.executesql(sql)

            for nd in root.findall('./location'):
                if nd is not None:
                    name = getXmlText(nd, './facility/name')
                    city = getXmlText(nd, './facility/address/city')
                    state = getXmlText(nd, './facility/address/state')
                    zip_code = getXmlText(nd, './facility/address/zip')
                    country = getXmlText(nd, './facility/address/country')
                    status = getXmlText(nd, './status')
                    sql = "INSERT INTO Location(studyid, name, city, state, zip_code, country, status) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}');".format(
                        studyid, name, city, state, zip_code, country, status)
                    cur.executesql(sql)

            nd = root.find('./sponsors/lead_sponsor')
            sponsor_name = getXmlText(nd, './agency')
            sponsor_class = getXmlText(nd, './agency_class')
            sql = "INSERT INTO Sponsor(studyid, agency, lead, class) VALUES('{0}', '{1}', '{2}', '{3}');".format(
                studyid, sponsor_name, 'Y', sponsor_class)
            cur.executesql(sql)

            for nd in root.findall('./location/investigator'):
                if nd is not None:
                    first_name = getXmlText(nd, './first_name')
                    middle_name = getXmlText(nd, './middle_name')
                    last_name = getXmlText(nd, './last_name')
                    degrees = getXmlText(nd, './degrees')
                    role = getXmlText(nd, './role')
                    affiliation = getXmlText(nd, './affiliation')
                    sql = "INSERT INTO Contact(studyid, first_name, middle_name, last_name, degrees, role, affiliation, contact_type) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');".format(
                        studyid, first_name, middle_name, last_name, degrees, role, affiliation, 'investigator')
                    cur.executesql(sql)

            nd = root.find('./overall_official')
            if nd is not None:
                first_name = getXmlText(nd, './first_name')
                middle_name = getXmlText(nd, './middle_name')
                last_name = getXmlText(nd, './last_name')
                degrees = getXmlText(nd, './degrees')
                role = getXmlText(nd, './role')
                affiliation = getXmlText(nd, './affiliation')
                sql = "INSERT INTO Contact(studyid, first_name, middle_name, last_name, degrees, role, affiliation, contact_type) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');".format(
                    studyid, first_name, middle_name, last_name, degrees, role, affiliation, 'overall_official')
                cur.executesql(sql)

            nd = root.find('./overall_contact')
            if nd is not None:
                first_name = getXmlText(nd, './first_name')
                middle_name = getXmlText(nd, './middle_name')
                last_name = getXmlText(nd, './last_name')
                degrees = getXmlText(nd, './degrees')
                role = getXmlText(nd, './role')
                affiliation = getXmlText(nd, './affiliation')
                sql = "INSERT INTO Contact(studyid, first_name, middle_name, last_name, degrees, role, affiliation, contact_type) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');".format(
                    studyid, first_name, middle_name, last_name, degrees, role, affiliation, 'overall_contact')
                cur.executesql(sql)

            nd = root.find('./overall_contact_backup')
            if nd is not None:
                first_name = getXmlText(nd, './first_name')
                middle_name = getXmlText(nd, './middle_name')
                last_name = getXmlText(nd, './last_name')
                degrees = getXmlText(nd, './degrees')
                role = getXmlText(nd, './role')
                affiliation = getXmlText(nd, './affiliation')
                sql = "INSERT INTO Contact(studyid, first_name, middle_name, last_name, degrees, role, affiliation, contact_type) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');".format(
                    studyid, first_name, middle_name, last_name, degrees, role, affiliation, 'overall_contact_backup')
                cur.executesql(sql)
            # Commit this entry
            con.commit()
con.close()