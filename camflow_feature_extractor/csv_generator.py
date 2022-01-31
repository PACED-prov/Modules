import pandas as pd
import numpy as np
import json
import sys
import os

# global vars
EDGES = []
VERTICES = []
HEADER = ["object_type", "entity_path", "reader_path", "writer_path", "namespaces", "reader_relation_types", "writer_relation_types", "writer_argvs", "reader_argvs"]
FEATURES = pd.DataFrame(columns=HEADER)
'''
HEADER:
    object_type             : (val) object type of center entity
    entity_path             : (list) paths associated to center entity
    reader_path             : (list) paths associated to readers of the center entity
    writer_path             : (list) paths associated to writers of the center entity
    namespaces              : (list) list of differing namespaces between readers and writers - [ipcns, mountns, netns, pidns, utsns] - if namespace differs = 1
    reader_relation_types   : (list) relation types of the readers of the the center entity
    writer_relation_types   : (list) relation types of the writers of the the center entity

    * center entity is the object on which the crossnamespace event is happening. There is only one center entity per json file.
'''
CENTER_ENTITY = None

# Adds center entity json obj to global variable
def set_center_entity(filepath):
    global VERTICES, CENTER_ENTITY

    # Extracting boot_id, machine_id, object_id.
    # input: "6_1851734740_22675_graph.json"
    # output [6,1851734740,22675]
    ids = filepath.split(".")[0].split("_")[:-1]
    ids[1] = "cf:" + ids[1]

    # updating center entity globally
    for v in VERTICES:
        if v["annotations"]["boot_id"] == ids[0] and v["annotations"]["cf:machine_id"] == ids[1] and v["annotations"]["object_id"] == ids[2]:
            CENTER_ENTITY = v


# Returns the object type of center entity
def extract_object_type():
    global CENTER_ENTITY

    object_type = ""

    try:
        object_type = CENTER_ENTITY["annotations"]["object_type"]
    except:
        pass

    return object_type

# Returns the paths associated to center entity
def extract_entity_path():
    global VERTICES, EDGES, CENTER_ENTITY

    paths = []

    ids_of_path_vertices = []

    for e in EDGES:
        # conditions
        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        to_path_vertex = e["annotations"]["to_type"] == "path"

        # if edge is from center entity to path vertex
        if  from_center_entity and to_path_vertex:
            ids_of_path_vertices.append(e["to"])

    # Getting pathnames
    for v in VERTICES:
        if v["id"] in ids_of_path_vertices:
            paths.append(v["annotations"]["pathname"])


    return list(set(paths))

# Returns the paths associated to readers of the center entity
def extract_reader_path():
    global VERTICES, EDGES, CENTER_ENTITY

    paths = []

    # Getting ids of reading processes - type == Used
    ids_from_type_used = []

    for e in EDGES:
        # conditions
        to_center_entity = e["to"] == CENTER_ENTITY["id"]

        edge_type_used = e["type"] == "Used"

        if to_center_entity and edge_type_used:
            ids_from_type_used.append(e["from"])

    # Getting ids of process memories connected to all readers
    ids_of_process_memory_vertices = []

    for e in EDGES:
        # conditions
        from_process_memory_vertex = e["annotations"]["from_type"] == "process_memory"

        to_any_reader = e["to"] in ids_from_type_used

        if from_process_memory_vertex and to_any_reader:
            ids_of_process_memory_vertices.append(e["from"])

    # Getting ids of path vertices connected to process memories
    ids_of_path_vertices = []

    for e in EDGES:
        #conditions
        from_process_memory_vertex = e["from"] in ids_of_process_memory_vertices

        to_path_vertex = e["annotations"]["to_type"] == "path"

        if from_process_memory_vertex and to_path_vertex:
            ids_of_path_vertices.append(e["to"])

    # Getting pathnames
    for v in VERTICES:
        if v["id"] in ids_of_path_vertices:
            paths.append(v["annotations"]["pathname"])

    # In case of central entity being process memory, it is possible that the path of writers is same as central entity
    if paths == []:
        paths = extract_entity_path()

    return list(set(paths))

# Returns the paths associated to writers of the center entity
def extract_writer_path():
    global VERTICES, EDGES, CENTER_ENTITY

    paths = []

    # Getting ids of writing processes - type == WasGeneratedBy
    ids_to_type_WGB = []

    for e in EDGES:
        # conditions
        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        edge_type_WGB = e["type"] == "WasGeneratedBy"

        if from_center_entity and edge_type_WGB:
            ids_to_type_WGB.append(e["to"])

    # Getting ids of process memories connected to all writers
    ids_of_process_memory_vertices = []

    for e in EDGES:
        # conditions
        to_process_memory_vertex = e["annotations"]["to_type"] == "process_memory"

        from_any_writer = e["from"] in ids_to_type_WGB

        if to_process_memory_vertex and from_any_writer:
            ids_of_process_memory_vertices.append(e["to"])

    # Getting ids of path vertices connected to process memories
    ids_of_path_vertices = []

    for e in EDGES:
        #conditions
        from_process_memory_vertex = e["from"] in ids_of_process_memory_vertices

        to_path_vertex = e["annotations"]["to_type"] == "path"

        if from_process_memory_vertex and to_path_vertex:
            ids_of_path_vertices.append(e["to"])

    # Getting pathnames
    for v in VERTICES:
        if v["id"] in ids_of_path_vertices:
            paths.append(v["annotations"]["pathname"])

    # In case of central entity being process memory, it is possible that the path of writers is same as central entity
    if paths == []:
        paths = extract_entity_path()

    return list(set(paths))

# Returns the list of differing namespaces between readers and writers
# ipcns, mountns, netns, pidns, utsns
def extract_namespaces():
    global VERTICES, EDGES, CENTER_ENTITY

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
                r_ns = (v['annotations']['ipcns'], v['annotations']['mntns'], v['annotations']['netns'], v['annotations']['pidns'], v['annotations']['utsns'])

                for i in range (5):
                    reader_ns[i].add(r_ns[i])

        for id in ids_to_type_WGB:
            if (v['id'] == id):
                w_ns = (v['annotations']['ipcns'], v['annotations']['mntns'], v['annotations']['netns'], v['annotations']['pidns'], v['annotations']['utsns'])

                for i in range (5):
                        writer_ns[i].add(w_ns[i])

    one_hot = [0, 0, 0, 0, 0]

    for i in range (5):
       diff = reader_ns[i].symmetric_difference(writer_ns[i])

       if len(diff) != 0:
           one_hot[i] = 1

    return one_hot




# Returns the relation types of the readers of the the center entity
def extract_reader_relation_types():
    global VERTICES, EDGES, CENTER_ENTITY

    # Getting ids of reading processes - type == Used
    reader_relation_types = []

    for e in EDGES:
        # conditions
        to_center_entity = e["to"] == CENTER_ENTITY["id"]

        edge_type_used = e["type"] == "Used"

        if to_center_entity and edge_type_used:
            reader_relation_types.append(e["annotations"]["relation_type"])


    return list(set(reader_relation_types))

# Returns the relation types of the writers of the the center entity
def extract_writer_relation_types():
    global VERTICES, EDGES, CENTER_ENTITY

    # Getting ids of writing processes - type == WasGeneratedBy
    writer_relation_types = []

    for e in EDGES:
        # conditions
        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        edge_type_WGB = e["type"] == "WasGeneratedBy"

        if from_center_entity and edge_type_WGB:
            writer_relation_types.append(e["annotations"]["relation_type"])

    return list(set(writer_relation_types))

def extract_writer_argvs():
    global VERTICES, EDGES, CENTER_ENTITY

    argvs = []

    # Getting ids of writing processes - type == WasGeneratedBy 
    ids_to_type_WGB = []

    for e in EDGES:
        # conditions
        from_center_entity = e["from"] == CENTER_ENTITY["id"]

        edge_type_WGB = e["type"] == "WasGeneratedBy"

        if from_center_entity and edge_type_WGB:
            ids_to_type_WGB.append(e["to"])

    # Getting ids of process memories connected to all writers
    ids_of_process_memory_vertices = []

    for e in EDGES:
        # conditions
        to_process_memory_vertex = e["annotations"]["to_type"] == "process_memory"

        from_any_writer = e["from"] in ids_to_type_WGB

        if to_process_memory_vertex and from_any_writer:
            ids_of_process_memory_vertices.append(e["to"])

    # Getting ids of argv vertices connected to process memories
    ids_of_argv_vertices = []

    for e in EDGES:
        #conditions
        from_process_memory_vertex = e["from"] in ids_of_process_memory_vertices

        to_path_vertex = e["annotations"]["to_type"] == "argv"

        if from_process_memory_vertex and to_path_vertex:
            ids_of_argv_vertices.append(e["to"])

    # Getting values of argvs
    for v in VERTICES:
        if v["id"] in ids_of_argv_vertices:
            argvs.append(v["annotations"]["value"])

    # In case of central entity being process memory, it is possible that the path of writers is same as central entity
    # if paths == []:
    #     paths = extract_entity_path()

    argvs = list(set(argvs))
    return argvs

def extract_reader_argvs():
    global VERTICES, EDGES, CENTER_ENTITY

    argvs = []

    # Getting ids of reading processes - type == Used 
    ids_from_type_used = []

    for e in EDGES:
        # conditions
        to_center_entity = e["to"] == CENTER_ENTITY["id"]

        edge_type_used = e["type"] == "Used"

        if to_center_entity and edge_type_used:
            ids_from_type_used.append(e["from"])

    # Getting ids of process memories connected to all readers
    ids_of_process_memory_vertices = []

    for e in EDGES:
        # conditions
        from_process_memory_vertex = e["annotations"]["from_type"] == "process_memory"

        to_any_reader = e["to"] in ids_from_type_used

        if from_process_memory_vertex and to_any_reader:
            ids_of_process_memory_vertices.append(e["from"])

    # Getting ids of argv vertices connected to process memories
    ids_of_argv_vertices = []

    for e in EDGES:
        #conditions
        from_process_memory_vertex = e["from"] in ids_of_process_memory_vertices

        to_path_vertex = e["annotations"]["to_type"] == "argv"

        if from_process_memory_vertex and to_path_vertex:
            ids_of_argv_vertices.append(e["to"])

    # Getting pathnames
    for v in VERTICES:
        if v["id"] in ids_of_argv_vertices:
            argvs.append(v["annotations"]["value"])

    # # In case of central entity being process memory, it is possible that the path of writers is same as central entity
    # if paths == []:
    #     paths = extract_entity_path()

    argvs = list(set(argvs))
    return argvs

def load_data(filepath):
    global EDGES, VERTICES
    
    print("Reading json file:", filepath)

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

    print("Done")


def main():
    global EDGES, VERTICES, FEATURES

    files = []

    with open("filenames.txt", "r") as f:
        files = f.readlines()

    for file in files:
        load_data(file.strip())

        set_center_entity(file.strip())

        object_type = extract_object_type()
        entity_path = extract_entity_path()
        reader_path = extract_reader_path()
        writer_path = extract_writer_path()
        namespaces  = extract_namespaces()
        reader_relation_types = extract_reader_relation_types()
        writer_relation_types = extract_writer_relation_types()
        writer_argvs = extract_writer_argvs()
        reader_argvs = extract_reader_argvs()

        data_point = {HEADER[0]: object_type, 
                    HEADER[1]: entity_path, 
                    HEADER[2]: reader_path, 
                    HEADER[3]: writer_path, 
                    HEADER[4]: namespaces, 
                    HEADER[5]: reader_relation_types, 
                    HEADER[6]: writer_relation_types,
                    HEADER[7]: writer_argvs,
                    HEADER[8]: reader_argvs
                    }
        
        FEATURES = FEATURES.append(data_point, ignore_index = True)

        #print(FEATURES)
        print("********** " + str(counter) + " JSON file(s) processed **********\n")
        counter = counter+ 1

    FEATURES.to_csv("features.csv", index=False)


if __name__ == '__main__':
    try:
        print("Starting...")
        main()
    except KeyboardInterrupt:
        print("Exiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
