import pandas as pd
import numpy as np
import sys
import os

'''
 --------------------------------------------------------------------------------
 @What it does?
    The following Python module is designed to ingest a JSON file output from
    the SPADE's CrossNamespaces filter and a query_template. It then extracts
    all the readers and writers for each entity from the JSON file and combines
    them together through SPADE constraints for each entity. Then it outputs
    SPADE queries to write these constraints along with the complete context
    w.r.t process_memory, path, and argv vertices. 
 
 @When should you use it?
    If you wish to create cross-namespace flow graphs for multiple entities 
    across numerous cross-namespace events then this Python module can help
    you create an iterative SPADE query for each entity that you can load into
    SPADE query client using the following command: load <query_file_path>
 
 @authors Shahpar Khan, Mashal Abbas
 --------------------------------------------------------------------------------
'''

# Function to create list of constraints broken down in multiple variables
def list_constraint(full_constraint, constraint_name):
    result = []
    compound_result = []
    
    char_index = 0
    mod_count = 0
    start = 0
    constraint_count = 0
    or_index = None
    
    while(char_index < len(full_constraint)):

        if ((mod_count % 1024 > 511)):

            for reverse_index in range(char_index - 4, 0, -1):
                if (full_constraint[reverse_index:reverse_index+4] == " or "):
                    or_index = reverse_index
                    break

            resultant_constraint = constraint_name + str(constraint_count) + " = " + full_constraint[start:or_index]
            result.append(resultant_constraint)
            
            compound_result.append(constraint_name + str(constraint_count))

            constraint_count = constraint_count + 1
            start = or_index + 4
            mod_count = 0
        char_index = char_index + 1
        mod_count = mod_count + 1

    
    compound_result.append(constraint_name + str(constraint_count))
    
    resultant_constraint = constraint_name + str(constraint_count) + " = " + full_constraint[start:]
    result.append(resultant_constraint)

    compound_result_or = " or ".join(compound_result)
    compound_result_and = " and ".join(compound_result)

    result = "\n\n".join(result)
    
    return result, compound_result_or, compound_result_and


# Function to create dataframes from the input JSON file
def createDataframes(json_file_path):
    
    # Reading JSON file
    df = pd.read_json(json_file_path, lines=True)

    df_artifact = pd.json_normalize(df['artifact'])
    df_artifact = df_artifact.rename(columns={"boot_id": "entity_boot_id", "cf:machine_id": "entity_cf:machine_id", "object_id": "entity_object_id"})

    df_reader = pd.json_normalize(df['reader'])

    df_artifact_reader = pd.concat([df_artifact, df_reader], axis=1)

    identifier_columns = ['entity_boot_id', 'entity_cf:machine_id', 'entity_object_id']

    df_artifact_reader = df_artifact_reader.set_index(identifier_columns).sort_index()
    df_artifact_reader_unique = df_artifact_reader.index.unique()
    dict_dfs_readers = {}
    for i in range(len(df_artifact_reader_unique)):
        all_readers_of_identifier = df_artifact_reader.loc[df_artifact_reader_unique[i]]
        all_readers_of_identifier = all_readers_of_identifier[['id']]
        dict_dfs_readers[df_artifact_reader_unique[i]] = all_readers_of_identifier

    # extracting writers
    list_of_list_of_writiers = []
    for i in range(df.shape[0]):
        df_writers_normalized = pd.json_normalize(df.iloc[i]['writers'])
        df_writers_normalized['index'] = i
        list_of_list_of_writiers.append(df_writers_normalized)
    
    writer_df = pd.concat(list_of_list_of_writiers).set_index('index')
    writer_df = writer_df.add_prefix('writer_') 

    merged_artifact_writer_df = pd.merge(df_artifact, writer_df, left_index=True, right_index=True)
    merged_artifact_writer_df = merged_artifact_writer_df.drop_duplicates().set_index(identifier_columns)

    merged_artifact_writer_df_unique = merged_artifact_writer_df.index.unique()
    list_dfs_writers= []
    for i in range(len(merged_artifact_writer_df_unique)):
        all_writers_of_identifier = merged_artifact_writer_df.loc[[merged_artifact_writer_df_unique[i]]]
        try:
            list_dfs_writers.append(all_writers_of_identifier.to_frame().reset_index())
        except:
            list_dfs_writers.append(all_writers_of_identifier.reset_index())


    return list_dfs_writers, dict_dfs_readers


# Function to load general query template
def loadQueryTemplate(query_template_path):

    query_template = ""

    with open(query_template_path, 'r') as f:
        lines = f.readlines()
        query_template = "".join(lines)

    return query_template


# Function to create general SPADE variable that will be used in each entity graph
def writeGeneralSpadeVars(output_query_file):
    output_query_file.write("# Creating a new $base2 with dropped duplicate vertices and repeated edges w.r.t. relation_type\n")
    output_query_file.write("$base2 = $base.collapseEdge('relation_type')\n")

    output_query_file.write("# Group all process memory vertices which contain the namespace identifiers.\n")
    output_query_file.write("$memorys = $base2.getVertex(object_type = 'process_memory')\n")

    output_query_file.write("# Group all task vertices which contain the process identifiers. Tasks are connected to process memory vertices.\n")
    output_query_file.write("$tasks = $base2.getVertex(object_type = 'task')\n")

    output_query_file.write("# Group all path vertices which contain the path of an inode in the filesystem. Files are connected to paths.\n")
    output_query_file.write("$paths = $base2.getVertex(object_type = 'path')\n")

    output_query_file.write("# Group all argv vertices which contain the argument passed to a process.\n")
    output_query_file.write("$argvs = $base2.getVertex(object_type = 'argv')\n\n")


# Function to create entity, reader, and writer according to SPADE query surface
def createConstraints(df, dict_dfs_readers):

    entity_tuple = (str(df.iloc[0]['entity_boot_id']), str(df.iloc[0]['entity_cf:machine_id']), str(df.iloc[0]['entity_object_id'] ))
    entity_constraint = "%entity_constraint = \"boot_id\" == '" + df.iloc[0]['entity_boot_id'] + "' and \"cf:machine_id\" == '" + df.iloc[0]['entity_cf:machine_id']  + "' and \"object_id\" == '" + df.iloc[0]['entity_object_id']  + "'\n"
    cross_entities = "\n$crossnamespace_entities = $base2.getVertex(%entity_constraint)\n\n"
    
    if df.shape[0] > 500:
        df = df.head(500)

    # constructing writer constraint
    df['combined'] = "\"id\" == '" + df['writer_id'] + "'"
    
    writer_concat_string = ' or '.join(df['combined'])
    writer_constraint, writer_compound_result_or, _ = list_constraint(writer_concat_string, "%writer_constraint")
    cross_writers = "\n\n$crossnamespace_writers = $base2.getVertex(" + writer_compound_result_or + ")\n\n"

    # constructing reader constraint
    reader_df = dict_dfs_readers[entity_tuple]

    if reader_df.shape[0] > 500:
        reader_df = reader_df.head(500)
    reader_df['combined'] = "\"id\" == '" + reader_df['id'] + "'"

    reader_concat_string = ' or '.join(reader_df['combined'])

    reader_constraint, reader_compound_result_or, _ = list_constraint(reader_concat_string, "%reader_constraint")
    cross_readers = "\n\n$crossnamespace_readers = $base2.getVertex(" + reader_compound_result_or + ")\n\n"

    return entity_tuple, entity_constraint, cross_entities, reader_constraint, cross_readers, writer_constraint, cross_writers


# Function to create SPADE queries that output the graph created in SPADE query client
def createOutputQueries(entity_tuple, counter):
    dot_name = "\n\nexport > /home/vagrant/output_graph/" + entity_tuple[0] + "_" + entity_tuple[1][3:] + "_" + entity_tuple[2] + "_" + str(counter) + "_graph.dot"
    json_name = "\n\nexport > /home/vagrant/output_graph/" + entity_tuple[0] + "_" + entity_tuple[1][3:] + "_" + entity_tuple[2] + "_" + str(counter) + "_graph.json"
    subgraph_dump = "\n\ndump all $transformed_subgraph"

    return dot_name, json_name, subgraph_dump


# Function to write respective graph queries to the query file
def writeGraphQueries(output_query_file, reset_workspace, entity_constraint, cross_entities, reader_constraint, cross_readers, writer_constraint, cross_writers, query_template, dot_name, subgraph_dump, json_name):
    output_query_file.write(reset_workspace)

    output_query_file.write(entity_constraint)
    output_query_file.write(cross_entities)

    output_query_file.write(reader_constraint)
    output_query_file.write(cross_readers)

    output_query_file.write(writer_constraint)
    output_query_file.write(cross_writers)

    output_query_file.write(query_template)

    output_query_file.write(dot_name)
    output_query_file.write(subgraph_dump)

    output_query_file.write(json_name)
    output_query_file.write(subgraph_dump)


def main(input_json_path, query_template_path, output_query_filename):

    # Loading static query template
    query_template = loadQueryTemplate(query_template_path)

    # Creating list of writer dataframes and dictionary of reader dataframes
    list_dfs_writers, dict_dfs_readers = createDataframes(input_json_path)
    
    # Opening output query file
    output_query_file = open(output_query_filename, 'a')

    # Writing general SPADE variables to the query file
    writeGeneralSpadeVars(output_query_file)

    counter = 0
    variables_to_erase = "$crossnamespace_entities $crossnamespace_writers $crossnamespace_readers $connected_entities $crossnamespace_flow_0 $crossnamespace_flow_1 $crossnamespace_path_vertices $crossnamespace_path $writing_process_memory $reading_process_memory $writing_task_to_writing_memory $reading_memory_to_reading_task $writing_process_memory_all_versions $reading_process_memory_all_versions $writing_process_memory_path $reading_process_memory_path $writing_process_to_argv $reading_process_to_argv $subgraph $transformed_subgraph"

    # Constructing query for each entity in a single query file
    for df in list_dfs_writers:

        counter = counter + 1

        # Creating entity, reader and, writer constraints
        entity_tuple, entity_constraint, cross_entities, reader_constraint, cross_readers, writer_constraint, cross_writers = createConstraints(df, dict_dfs_readers)

        # Constructing output svg path, svg dump command, and reset workspace command
        dot_name, json_name, subgraph_dump = createOutputQueries(entity_tuple, counter)
        reset_workspace = "\n\n########## Graph number: " + str(counter) + " ##########\n\nerase " + variables_to_erase + "\n\n"

        # Writing all the queries for the current entity to the query file
        writeGraphQueries(output_query_file, reset_workspace, entity_constraint, cross_entities, reader_constraint, cross_readers, writer_constraint, cross_writers, query_template, dot_name, subgraph_dump, json_name)

        print("Graph number: " + counter + " done...")

    output_query_file.close()

    print("All graph queries completed...")


if __name__ == '__main__':
    try:
        if len(sys.argv) != 4:
            raise Exception("run python3 sortlog.py <input_json_path> <query_template_path> <output_query_filename>")
        else:
            print("Starting...")
            print("Input json path:", sys.argv[1])
            print("Query template path:", sys.argv[2])
            print("Output query filename:", sys.argv[3])
            main(sys.argv[1], sys.argv[2], sys.argv[3])
        
    except KeyboardInterrupt:
        print("Exiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
