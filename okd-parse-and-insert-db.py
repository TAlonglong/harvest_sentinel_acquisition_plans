from datetime import datetime, timedelta
from lxml import etree
import mysql.connector
import hashlib

# Platform name translater. Key must be upcase.
# The value must be lowercase of the name in the tle files
platform_name_tr = {'S1A': 'sentinel 1a',
                    'S1B': 'sentinel 1b',
                    'S1C': 'sentinel 1c',
                    'S1D': 'sentinel 1d'}

pytroll_pass = 'pytroll_pass_test'

add_sentinel_schedule = ("insert into {} (satellite_name, aos, los, schedule, pass_key ) "
                         "values (%s, %s, %s, %s, %s)".format(pytroll_pass))


def insert_into_db(orb):
    platform_name = orb['SatelliteId']
    sentinel_schedule_id = 0
    if orb['Mode'] == 'EW' and orb['Polarisation'] == 'DH':
        sentinel_schedule_id = 24
    elif orb['Mode'] == 'IW' and orb['Polarisation'] == 'DH':
        sentinel_schedule_id = 25
    elif orb['Mode'] == 'IW' and orb['Polarisation'] == 'DV':
        sentinel_schedule_id = 26
    else:
        print("Unknown schedule. ", orb)
        return 0

    md5_hash = hashlib.md5()
    md5_hash.update(('{}{}{}{}'.format(aos, los, platform_name, sentinel_schedule_id)).encode('utf-8'))
    print("Sentinel schedule: ", aos, los, platform_name_tr.get(platform_name.upper(), platform_name))

    try:
        # Find the mid time of the pass. This will be used to compare to the mid_time from the
        # scheduled passes. If the difference is less than 60 seconds it is assumed to be the same
        # pass. IE, if the change of the pass is larger than 60 seconds it will not be found.
        # Also if a pass is deleted from the ESA schedule, the pass in the db will not be deleted.
        mid_time = aos + (los - aos) / 2

        cnx = mysql.connector.connect(user='polarsat', password='lilla land',
                                      host='satproc3',
                                      database='orbits')
        schedule_check = cnx.cursor(dictionary=True)

        mysql_search = ("select * from {} where satellite_name=\"{}\" and "
                        "abs(unix_timestamp('{}')-(unix_timestamp(aos)+"
                        "(unix_timestamp(los)-unix_timestamp(aos))/2))<60 and "
                        "(schedule={})".format(pytroll_pass,
                                               platform_name_tr.get(platform_name.upper(),
                                                                    platform_name),
                                               mid_time,
                                               sentinel_schedule_id))

        schedule_check.execute(mysql_search)
        schedules = schedule_check.fetchall()

        insert = True
        update = None
        if len(schedules) == 0:
            print("Found no schedules.")
        elif len(schedules) == 1:
            for sched in schedules:
                insert = False
                if sched['pass_key'] == md5_hash.hexdigest():
                    print("Is same pass/schedule. Leave as is.")
                else:
                    print("DB schedule:   ", sched['aos'], sched['los'], sched['satellite_name'])
                    print(sched)
                    print("Is same pass but slightly changed. Need to update.")
                    update = sched['Id']
        else:
            print("Found more than 1 duplicate passes. This should not happen.")
            print(schedules)

        if insert:
            cursor = cnx.cursor(dictionary=True)
            cursor.execute(add_sentinel_schedule, (platform_name_tr[platform_name.upper()],
                                                   aos, los, sentinel_schedule_id, md5_hash.hexdigest()))
            cnx.commit()
            print("Connected to and inserted into db.")
            cursor.close()
        elif update:
            update_statement = ("update {} set satellite_name='{:s}', "
                                "aos='{:s}', los='{:s}', "
                                "schedule='{:s}', pass_key='{:s}' "
                                "where Id={}".
                                format(pytroll_pass,
                                       platform_name_tr[platform_name.upper()],
                                       aos.strftime("%Y-%m-%d %H:%M:%S"),
                                       los.strftime("%Y-%m-%d %H:%M:%S"),
                                       str(sentinel_schedule_id),
                                       md5_hash.hexdigest(),
                                       update))
            insert_pass = cnx.cursor(dictionary=True)
            insert_pass.execute(update_statement)
            cnx.commit()
            print("No updates: {}".format(insert_pass.rowcount))
            print("Id of last updated: {}".format(update))
        else:
            print("No changes.")

    except mysql.connector.IntegrityError as e:
        if e.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
            print("Pass already in db. Skip this")
            pass
    except mysql.connector.Error as err:
        print("mysql connect failed with: {}".format(err))
    finally:
        cnx.close()
    return 1


kml_files = ['./S1A_acquisition_plan_norwAOI.kml',
             './S1B_acquisition_plan_norwAOI.kml']

all_passes = {}
for kml_file in kml_files:
    tree = etree.parse(kml_file)

    root = tree.getroot()

    nsmap = root.nsmap[None]
    find_prefix = './/{' + nsmap + '}'

    for pm in tree.findall(find_prefix + 'Placemark'):
        e_data = pm.find(find_prefix + 'ExtendedData')
        reg = {}
        for attr in ['SatelliteId', 'DatatakeId', 'Mode', 'Swath', 'Polarisation', 'ObservationTimeStart',
                     'ObservationTimeStop', 'ObservationDuration', 'OrbitAbsolute', 'OrbitRelative']:
            it_data = e_data.find(find_prefix + "Data[@name='{}']".format(attr))
            v = it_data.find(find_prefix + "value")
            value = v.text
            if 'ObservationTime' in attr:
                value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
            reg[attr] = value
        if reg['SatelliteId'] not in all_passes:
            all_passes[reg['SatelliteId']] = {}

        if reg['OrbitAbsolute'] not in all_passes[reg['SatelliteId']]:
            all_passes[reg['SatelliteId']][reg['OrbitAbsolute']] = []

        all_passes[reg['SatelliteId']][reg['OrbitAbsolute']].append(reg)

now = datetime.now()
midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = midnight + timedelta(days=1)
the_day_after_tomorrow = tomorrow + timedelta(days=1)
for satid in all_passes:
    for orbit in sorted(all_passes[satid]):
        added = False
        for orb in all_passes[satid][orbit]:
            aos = orb['ObservationTimeStart']
            los = orb['ObservationTimeStop']
            if (aos >= midnight and los < the_day_after_tomorrow):
                print("Start {}, end {}, Mode {}, pol {}, sat {}".format(orb['ObservationTimeStart'],
                                                                         orb['ObservationTimeStop'],
                                                                         orb['Mode'],
                                                                         orb['Polarisation'],
                                                                         orb['SatelliteId']))
                if insert_into_db(orb):
                    added = True
        if added:
            print("-------------------------------")
