import sys
import getopt
import aerospike


HELP = 'aerospike-helper.py -o <operation> -h <host> -p <port> -n <namespace> -s <set>'


def print_result((key, metadata, record)):
        print key, "\n", metadata, record, "\n"


def main(argv):
    operation = 'scan'
    host = '127.0.0.1'
    port = 3000
    timeout = 1000
    namespace = "test"
    set = "SET"
    try:
        opts, args = getopt.getopt(argv,"o:h:p:s:k:",["operation=", "host=","port=","namespace=","set="])
    except getopt.GetoptError:
        print HELP
        #sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print HELP
            #sys.exit(2)
        elif opt in ("-o", "--operation"):
            operation = arg
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--port") and arg.isdigit():
            port = arg
        elif opt in ("-n", "--namespace"):
            namespace = arg
        elif opt in ("-s", "--set"):
            set = arg
    config = {
        'hosts': [ (host, int(port))],
        'timeout': timeout
        }

    try:
        client = aerospike.client(config).connect()
    except Exception as ex:
        print ex
        import sys
        print "failed to connect to the cluster with ", config['hosts']
        sys.exit()

    if operation == 'put':
        aerospike_put(client, namespace, set)
    else:
        aerospike_scan(client, namespace, set)


def aerospike_put(client, namespace, set):
    key = "samplekey2"
    expanded_key = (namespace, set, key)
    try:
        client.put(
                key=expanded_key,
                bins={'key': key, 'name': 'John Does', 'age': 32},
                policy={'key':aerospike.POLICY_KEY_SEND},
                meta={'ttl': 600000}
            )
    except Exception as e:
        import sys
        print "Error: {0}".format(e)

    print_result(client.get(expanded_key))
    client.close()


def aerospike_scan(client, namespace, set):
    try:
        scan = client.scan(namespace, set)
        scan.select()
        scan.foreach(print_result)
    except Exception as e:
        import sys
        print "Error: {0}".format(e)
    client.close()


if __name__ == "__main__":
   main(sys.argv[1:])