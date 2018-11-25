from __future__ import unicode_literals
import argparse
import sys
import io
import json
import requests
from pprint import pprint
import MySQLdb as Mdb
import collections


# Needed to remove u infront of string marked as unicode
def _byteify(data, ignore_dicts=False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data


def write_json(file_path, data):
    try:
        with io.open(file_path, 'w') as outfile:
            content = json.dumps(data, separators=(',', ':'), ensure_ascii=True, indent=4)
            outfile.write(content)
            # json.dump(to_unicode(data), outfile, indent=4, ensure_ascii=False, encoding='utf-8')
            # json.dump(to_unicode(data), outfile)
            outfile.close()
    except IOError:
        print("Error writing file: " + IOError)
        sys.exit(1)


def extract_blueprint_json(file_path, cluster_name, url, user, password):
    headers = {"X-Requested-By": "ambari"}
    _url_ = "http://" + url + ":8080/api/v1/clusters/" + cluster_name + "?format=blueprint"
    data = requests.get(_url_, headers=headers, auth=(user, password))
    # json_data = json.loads(data.text)
    json_data = _byteify(json.loads(data.text, object_hook=_byteify), ignore_dicts=True)
    write_json(file_path + "/" + cluster_name + ".json", json_data)
    print("\nBlueprint downloaded: " + file_path + "/" + cluster_name + ".json")
    return json_data


def create_host_mapping(host_groups, db_url, db_user, db_pass, db_name, bounce_host=None, bounce_user=None,
                        bounce_password=None, bounce_key=None):

    print("Generating cluster host mapping........")
    con = None
    if bounce_host is not None:
        try:
            ###########################################
            from sshtunnel import SSHTunnelForwarder
            ###########################################
            tunnel = SSHTunnelForwarder((bounce_host, 22),
                                    ssh_username=bounce_user,
                                    ssh_password=bounce_password,
                                    ssh_private_key=bounce_key,
                                    remote_bind_address=(db_url, 3306),
                                    local_bind_address=('127.0.0.1', 3307))
            tunnel.start()
            con = Mdb.connect(host='127.0.0.1', port=3307, user=db_user, passwd=db_pass, db=db_name)
        except Exception as e:
            print("Error creating SSH tunnel: " + str(e))
        except Mdb.Error as e:
            print("Error DB connect: N:" + str(e))
            sys.exit(1)
    else:
        tunnel = None
        try:
            con = Mdb.connect(host=db_url, port=3306, user=db_user, passwd=db_pass, db=db_name)
        except Mdb.Error as e:
            print("Error DB connect: N:" + str(e))
            sys.exit(1)
    if con is None:
        print("Cannot Initialize DB connection. Check that you provided the right key file or password!")
        sys.exit(1)
    cursor = con.cursor()

    query = "SELECT host_id, host_name from " + db_name + ".hosts;"
    cursor.execute(query)
    result_tmp = cursor.fetchall()
    hosts = []
    for i in result_tmp:
        tmp = list(i)
        tmp[0] = str(tmp[0])
        hosts.append(tmp)

    print("Extracted hosts from Ambari Database")
    host_map = {}
    for host_group in host_groups:
        host_map[host_group['name']] = []

    print("Service mapping from Ambari database, which components run on each host")
    for host in hosts:
        query = "SELECT component_name from " + db_name + ".hostcomponentdesiredstate WHERE host_id=" + host[0] + ";"
        cursor.execute(query)
        host_components = cursor.fetchall()
        host_components = [x[0] for x in host_components]
        # print("host_components", host_components)
        for host_group in host_groups:
            json_host_components = [x['name'] for x in host_group['components'] if 'AMBARI' not in x['name']]
            # print("json_host_components:", json_host_components)
            check = lambda jhc, hc: collections.Counter(jhc) == collections.Counter(hc)
            compare = check(json_host_components, host_components)
            if compare:
                if len(host_map[host_group['name']]) < host_group['cardinality']:
                    host_map[host_group['name']].append(host[1])

    cursor.close()
    con.close()
    if tunnel is not None:
        tunnel.close()

    host_map_final = {"blueprint": cluster_name, "default_password": "Default_HDP_HDF", "host_groups": []}

    for name, value in host_map.iteritems():
        tmp = []
        for val in value:
            tmp.append({'fqdn': val})

        host_map_final['host_groups'].append({'name': name, 'hosts': tmp})

    host_map_final = json.dumps(host_map_final)

    host_map_final = _byteify(json.loads(host_map_final, object_hook=_byteify), ignore_dicts=True)
    # pprint(host_map_final)
    write_json(file_path + "/" + cluster_name + "_map.json", host_map_final)
    print("Host mapping generated: " + file_path + "/" + cluster_name + "_map.json\n")


def main():
    global file_path
    global cluster_name
    parser = argparse.ArgumentParser()

    parser.add_argument('-fp', '--file_path', nargs=1, help='Directory under which the json files will be saved')
    parser.add_argument('-cn', '--cluster_name', nargs=1, help='The name you gave to the cluster in Ambari')
    parser.add_argument('-ah', '--ambari_host', nargs=1, help='Ambari host url to get GET a blueprint')
    parser.add_argument('-au', '--ambari_user', nargs=1, help='Ambari username')
    parser.add_argument('-ap', '--ambari_password', nargs=1, help='Ambari password')
    parser.add_argument('-dh', '--database_host', nargs=1, help='Database host address')
    parser.add_argument('-du', '--database_user', nargs=1, help='Database user for Amabri db')
    parser.add_argument('-dp', '--database_password', nargs=1, help='Database password for Amabri db')
    parser.add_argument('-dn', '--database_name', nargs=1, help='Database name for Amabri db')
    parser.add_argument('-bh', '--bounce_host', nargs=1, help='Bounce host address')
    parser.add_argument('-bu', '--bounce_user', nargs=1, help='Bounce host user')
    parser.add_argument('-bp', '--bounce_pass', nargs=1, help='Bounce host password')
    parser.add_argument('-bk', '--bounce_key', nargs=1, help='Bounce host ssh key')

    args = parser.parse_args()

    if args.bounce_host:
        if len(sys.argv) == 25:
            file_path = args.file_path[0]
            cluster_name = args.cluster_name[0]
            ambari_host = args.ambari_host[0]
            ambari_user = args.ambari_user[0]
            ambari_password = args.ambari_password[0]
            database_host = args.database_host[0]
            database_user = args.database_user[0]
            database_password = args.database_password[0]
            database_name = args.database_name[0]
            bounce_host = args.bounce_host[0]
            bounce_user = args.bounce_user[0]
            if args.bounce_pass is not None:
                bounce_pass = args.bounce_pass[0]
                bounce_key = None
            else:
                bounce_pass = None
                bounce_key = args.bounce_key[0]

            json_data = extract_blueprint_json(file_path, cluster_name, ambari_host, ambari_user, ambari_password)
            create_host_mapping(json_data['host_groups'], database_host, database_user, database_password, database_name,
                                bounce_host, bounce_user, bounce_pass, bounce_key)
        else:
            print(parser.print_usage())
            sys.exit(1)
    else:
        if len(sys.argv) == 19:
            file_path = args.file_path[0]
            cluster_name = args.cluster_name[0]
            ambari_host = args.ambari_host[0]
            ambari_user = args.ambari_user[0]
            ambari_password = args.ambari_password[0]
            database_host = args.database_host[0]
            database_user = args.database_user[0]
            database_password = args.database_password[0]
            database_name = args.database_name[0]

            json_data = extract_blueprint_json(file_path, cluster_name, ambari_host, ambari_user, ambari_password)
            create_host_mapping(json_data['host_groups'], database_host, database_user, database_password, database_name)

        else:
            print(parser.print_usage())
            sys.exit(1)


if __name__ == '__main__':
    main()


# ambari_blueprint_extractor.py -fp ~/path/to/store/blueprint -cn ClusterName -ah ambari.server.your.domain -au admin \
# -ap admin_pass -dh database.server.your.domain -du ambari_user -dp ambari_password -dn ambari \
# -bh ssh.bounce(tunnel).host -bu user -bk /home/user/your_key.pem
