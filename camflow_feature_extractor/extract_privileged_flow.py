import pandas as pd
import json
import sys
import os

'''
 --------------------------------------------------------------------------------
 @What it does?
    The following Python module is designed to ingest a SPADE entity flow graph
    in a JSON format and output the priviledged flow flag based on the 
    the container engine (Docker or Kubernetes)
 
 @When should you use it?
    This Python module will help you mark the malicious entities with high
    accuracy that are involved in a container escape exploit.
 
 @authors 
    Shahpar Khan, Mashal Abbas
 --------------------------------------------------------------------------------
'''

# Global variables
EDGES = []
VERTICES = []
HEADER = ["bID_mID_oID", "priviledged_flow"]
FEATURES = pd.DataFrame(columns=HEADER)
ENTITY_COUNTS = {}
'''
HEADER:
    priviledged_flow    : (binary) 1 if there is a flow from low host to container; 0 otherwise.

    * center entity is the object on which the crossnamespace event is happening. There is only one center entity per json file.
'''
CENTER_ENTITY = None
HOST_IPCNS = None
CLUSTER_IPCNS = None
POLICY_NUMBER = None


def set_center_entity(filepath):
    global VERTICES, CENTER_ENTITY

    ids = filepath.split(".")[0].split("_")[:-1]
    ids[1] = "cf:" + ids[1]

    # updating center entity globally
    for v in VERTICES:
        if v["annotations"]["boot_id"] == ids[0] and v["annotations"]["cf:machine_id"] == ids[1] and v["annotations"]["object_id"] == ids[2]:
            CENTER_ENTITY = v


# Function to extract priviledged_flow for kubernetes environment
def extract_priviledge_flow_kubernetes():
    global VERTICES, EDGES, CENTER_ENTITY, HOST_IPCNS, CLUSTER_IPCNS, POLICY_NUMBER

    check_inter_pod_flows = False

    if POLICY_NUMBER == "2":
        print("Checking for inter pod flows...")
        check_inter_pod_flows = True

    #contains a list of the tuples of namespaces
    reader_ns = (set(), set(), set(), set(), set())
    writer_ns = (set(), set(), set(), set(), set())
    reader_ipc_pid_ns = set()
    writer_ipc_pid_ns = set()

    # Getting ids of reading processes - type == Used
    ids_from_type_used = []
    # Getting ids of writing processes - type == WasGeneratedBy
    ids_to_type_WGB = []

    for e in EDGES:
        # conditions
        to_center_entity = e["to"] == CENTER_ENTITY["id"]

        edge_type_used = e["type"] == "Used"

        if to_center_entity and edge_type_used:
            ids_from_type_used.append(e["from"])

        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        edge_type_WGB = e["type"] == "WasGeneratedBy"

        if from_center_entity and edge_type_WGB:
            ids_to_type_WGB.append(e["to"])

    for v in VERTICES:
        for id in ids_from_type_used:
            if (v['id'] == id):
                r_ns = (v['annotations']['ipcns'],)
                reader_ipc_pid_ns.add((v['annotations']['ipcns'], v['annotations']['pidns']))
                reader_ns[0].add(r_ns[0])

        for id in ids_to_type_WGB:
            if (v['id'] == id):
                w_ns = (v['annotations']['ipcns'],)
                writer_ipc_pid_ns.add((v['annotations']['ipcns'], v['annotations']['pidns']))
                writer_ns[0].add(w_ns[0])


    priviledged_flow = 0
    read_from_vm = False
    write_from_cluster = False
    write_from_vm = False
    read_from_cluster = False
    write_from_pod = False

    # Only a write from VM 
    if (len(writer_ns[0]) == 1) and (HOST_IPCNS in writer_ns[0]):
        priviledged_flow = 0
    else:
        for ns in writer_ns[0]:
            if ns == CLUSTER_IPCNS:
                write_from_cluster = True
            elif ns == HOST_IPCNS:
                write_from_vm = True
            else:
                write_from_pod = True
        
        for ns in reader_ns[0]:
            if ns == HOST_IPCNS:
                read_from_vm = True
            elif ns == CLUSTER_IPCNS:
                read_from_cluster = True

        if write_from_cluster and read_from_vm:
            priviledged_flow = 1
        
        if write_from_pod and read_from_vm:
            priviledged_flow = 1
        
        if write_from_pod and read_from_cluster:
            priviledged_flow = 1
        

        if(check_inter_pod_flows):
            if write_from_pod:

                # Exttacting only the pod (ipcns, pid) sets from reader and writer
                to_del = []
                for x in writer_ipc_pid_ns:
                    if x[0] == CLUSTER_IPCNS or x[0] == HOST_IPCNS:
                        to_del.append(x)
                for x in to_del:
                    writer_ipc_pid_ns.remove(x)
                
                to_del = []
                for x in reader_ipc_pid_ns:
                    if x[0] == CLUSTER_IPCNS or x[0] == HOST_IPCNS:
                        to_del.append(x)
                for x in to_del:
                    reader_ipc_pid_ns.remove(x)
                
                # checking if there exists a reader which differs from pod ns and vice versa
                for x in reader_ipc_pid_ns:
                    if x not in writer_ipc_pid_ns:
                        priviledged_flow = 1
                        break
            
    

    return priviledged_flow


# Function to extract priviledged_flow for docker environment
def extract_priviledge_flow_docker():
    global VERTICES, EDGES, CENTER_ENTITY, HOST_IPCNS

    #contains a list of the tuples of namespaces
    reader_ns = (set(), set(), set(), set(), set())
    writer_ns = (set(), set(), set(), set(), set())

    # Getting ids of reading processes - type == Used
    ids_from_type_used = []
    # Getting ids of writing processes - type == WasGeneratedBy
    ids_to_type_WGB = []

    for e in EDGES:
        # conditions
        to_center_entity = e["to"] == CENTER_ENTITY["id"]

        edge_type_used = e["type"] == "Used"

        if to_center_entity and edge_type_used:
            ids_from_type_used.append(e["from"])

        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        edge_type_WGB = e["type"] == "WasGeneratedBy"

        if from_center_entity and edge_type_WGB:
            ids_to_type_WGB.append(e["to"])

    for v in VERTICES:
        for id in ids_from_type_used:
            if (v['id'] == id):
                r_ns = (v['annotations']['ipcns'],)

                reader_ns[0].add(r_ns[0])

        for id in ids_to_type_WGB:
            if (v['id'] == id):
                w_ns = (v['annotations']['ipcns'],)

                writer_ns[0].add(w_ns[0])

    check_writer_container = False
    check_reader_host = False

    for ns in writer_ns[0]:
        if ns != HOST_IPCNS:
            check_writer_container = True

    for ns in reader_ns[0]:
        if ns == HOST_IPCNS:
            check_reader_host = True

    priviledged_flow = 0

    if check_reader_host and check_writer_container:
        priviledged_flow = 1
    

    return priviledged_flow


# Function to load a graph in a JSON format and store its vertices and edges globally
def load_data(filepath):
    global EDGES, VERTICES
    
    with open(filepath, "r") as f:
        for line in f:
            if "[" in line or "]" in line:
                continue

            if line[0] == ",":
                obj = json.loads(line[1:])
            else:
                obj = json.loads(line)

            if "from_type" in obj["annotations"]:
                EDGES.append(obj)
            else:
                VERTICES.append(obj)


# Function to extract the identifier <boot_id>_<cf:machine_id>_<object_id>
def extract_identifier():
    global CENTER_ENTITY

    return CENTER_ENTITY["annotations"]["boot_id"] + "_" + CENTER_ENTITY["annotations"]["cf:machine_id"].split(":")[1] + "_" + CENTER_ENTITY["annotations"]["object_id"]


def main(filepath, extract_priviledge_flow, host_ipcns, cluster_ipcns = None, policy = None):
    global EDGES, VERTICES, FEATURES, HOST_IPCNS, CLUSTER_IPCNS, POLICY_NUMBER

    HOST_IPCNS = host_ipcns
    CLUSTER_IPCNS = cluster_ipcns
    POLICY_NUMBER = policy

    files = []

    with open(filepath, "r") as f:
        files = f.readlines()

    counter = 1
    for file in files:
        load_data(file.strip())

        set_center_entity(file.strip())


        
        priviledged_flow = extract_priviledge_flow()
        bID_mID_oID = extract_identifier()

        data_point = {HEADER[0]: bID_mID_oID,
                      HEADER[1]: priviledged_flow,
        }
        
        FEATURES = FEATURES.append(data_point, ignore_index = True)

        print("********** " + str(counter) + " JSON file(s) processed **********\n")
        counter = counter+ 1

    FEATURES.to_csv("features.csv", index=False)


if __name__ == '__main__':
    exception_docker = "For Docker:\n\trun python3 csv_generator.py docker <filepath> <host_ipcns>"
    exception_kube = "For Kubernetes:\n\trun python3 csv_generator.py kube <filepath> <host_ipcns> <cluster_ipcns> <policy_number>"
    exception_msg = exception_docker + "\n" + exception_kube
    try:
        if len(sys.argv) < 2:
            raise Exception(exception_msg)
        else:

            if sys.argv[1] == "docker":
                if len(sys.argv) == 4:
                    print("Starting...")
                    print("Filepath:", sys.argv[2])
                    print("Host IPCNS:", sys.argv[3])
                    main(sys.argv[2], extract_priviledge_flow_docker, sys.argv[3])
                else:
                    raise Exception(exception_msg)

            elif sys.argv[1] == "kube":
                if len(sys.argv) == 6:
                    print("Starting...")
                    print("Filepath:", sys.argv[2])
                    print("Host IPCNS:", sys.argv[3])
                    print("Cluster IPCNS:", sys.argv[4])
                    print("Policy number:", sys.argv[5])
                    main(sys.argv[2], extract_priviledge_flow_kubernetes, sys.argv[3], sys.argv[4], sys.argv[5])
                else:
                    raise Exception(exception_msg)

            else:
                raise Exception(exception_msg)
        
        
    except KeyboardInterrupt:
        print("Exiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)