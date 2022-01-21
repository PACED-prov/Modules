import pandas as pd
import numpy as np
import json

WRITER_LIMIT = 2500

def extract_id(obj):
    return "\"id\" == " + "'" + obj["id"] + "'"

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



def main():

    global WRITER_LIMIT

    # reading filter output json file
    df = pd.read_json("media-docker-workload-only.json", lines=True)

    print("_____JSON file reading done_____")

    # loading static query template
    query_template = ""

    with open('./query_template', 'r') as f:
        lines = f.readlines()
        query_template = "".join(lines)
    
    print("_____Query template reading done_____")

    
    # Loading readers dfs for each entity into a dictionary 
    # where the key is the entity index tuple
    df_artifact = pd.json_normalize(df['artifact'])
    df_reader = pd.json_normalize(df['reader'])
    df_artifact = df_artifact.rename(columns={"boot_id": "entity_boot_id", "cf:machine_id": "entity_cf:machine_id", "object_id": "entity_object_id"})

    df_artifact_reader_writer = pd.concat([df_artifact, df_reader], axis=1)
    df_artifact_reader_writer["writers"] = list(df["writers"])
    # df_artifact_reader_writer["event-id"] = list(df["cross-namespace-event-id"])

    print("_____Intial dataframe construction done_____")
    print("\tNumber of rows:", df_artifact_reader_writer.shape[0])

    print("_____Starting query construction_____")

    for row in range(df_artifact_reader_writer.shape[0]):
        
        # Entity extraction
        entity_tuple = (str(df_artifact_reader_writer.iloc[row]['entity_boot_id']), str(df_artifact_reader_writer.iloc[row]['entity_cf:machine_id']), str(df_artifact_reader_writer.iloc[row]['entity_object_id'] ))
        entity_constraint = "%entity_constraint = \"boot_id\" == '" + entity_tuple[0] + "' and \"cf:machine_id\" == '" + entity_tuple[1]  + "' and \"object_id\" == '" + entity_tuple[2]  + "'\n"
        cross_entities = "\n$crossnamespace_entities = $base.getVertex(%entity_constraint)\n\n"

        # Reader extraction
        reader_constraint, reader_compound_result_or, _ = list_constraint(" or ".join(["\"id\" == '" + df_artifact_reader_writer.iloc[row]['id'] + "'"]), "%reader_constraint")
        cross_readers = "\n\n$crossnamespace_readers = $base.getVertex(" + reader_compound_result_or + ")\n\n"

        # Writer extraction
        writers = list(map(extract_id,df_artifact_reader_writer.iloc[0]['writers']))
        number_of_writers = len(writers)
        if number_of_writers > WRITER_LIMIT:
            print("Entity with object_id: \"" + entity_tuple[2] + "\" is skipped due to " + str(number_of_writers) + " writers. Limit is set to: " + WRITER_LIMIT)
            continue

        writer_constraint, writer_compound_result_or, _ = list_constraint(" or ".join(writers), "%writer_constraint")
        cross_writers = "\n\n$crossnamespace_writers = $base.getVertex(" + writer_compound_result_or + ")\n\n"        

        # Export, dump, and reset
        svg_name = "\n\nexport > /home/vagrant/transformed_graph/" + entity_tuple[0] + "_" + entity_tuple[1][3:] + "_" + entity_tuple[2] + "_" + str(row) + "_graph.json"
        svg_dump = "\n\ndump all $transformed_subgraph"
        reset_workspace = "\n\n########## Graph number: " + str(row) + " ##########\n\nreset workspace\n\n"


        # Writing to query file
        with open("individual_graph_query", 'a') as f:
            f.write(reset_workspace)

            f.write(entity_constraint)
            f.write(cross_entities)

            f.write(reader_constraint)
            f.write(cross_readers)

            f.write(writer_constraint)
            f.write(cross_writers)

            f.write(query_template)

            f.write(svg_name)
            f.write(svg_dump)

    print("_____Query construction done_____")
    print("_____Exiting_____")



    # Old code for per entity graph generation
    '''




    cols = ['entity_boot_id', 'entity_cf:machine_id', 'entity_object_id']

    df6 = df_artifact_reader.set_index(cols).sort_index()
    uni = df6.index.unique()
    dict_dfs_readers = {}
    for i in range(len(uni)):
        dfx = df6.loc[uni[i]]
        dfx = dfx[['id']]
        dict_dfs_readers[uni[i]] = dfx

    # extracting writers
    dfs = []
    for i in range(df.shape[0]):
        new_df = pd.json_normalize(df.iloc[i]['writers'])
        new_df['index'] = i
        dfs.append(new_df)
    
    writer_df = pd.concat(dfs).set_index('index')
    writer_df = writer_df.add_prefix('writer_') 

    merged = pd.merge(df_artifact, writer_df, left_index=True, right_index=True)
    merged = merged.drop_duplicates().set_index(cols)

    unique1 = merged.index.unique()
    list_dfs_writers= []
    for i in range(len(unique1)):
        dfx = merged.loc[[unique1[i]]]
        try:
            list_dfs_writers.append(dfx.to_frame().reset_index())
        except:
            list_dfs_writers.append(dfx.reset_index())


    counter = 0
    # constructing query for each entity in a single query file
    for df in list_dfs_writers:

        counter = counter + 1
        # constructing entity constraint
        # entity_tuple = df.index.unique()[0]
        # print(df.iloc[0])
        # print(counter)
        # print(df.iloc[0])
        # print(df.iloc[0]['entity_boot_id'])
        # print(df.iloc[0]['entity_cf:machine_id'])
        # print(df.iloc[0]['entity_object_id'])
        entity_tuple = (str(df.iloc[0]['entity_boot_id']), str(df.iloc[0]['entity_cf:machine_id']), str(df.iloc[0]['entity_object_id'] ))
        entity_constraint = "%entity_constraint = \"boot_id\" == '" + df.iloc[0]['entity_boot_id'] + "' and \"cf:machine_id\" == '" + df.iloc[0]['entity_cf:machine_id']  + "' and \"object_id\" == '" + df.iloc[0]['entity_object_id']  + "'\n"
        cross_entities = "\n$crossnamespace_entities = $base.getVertex(%entity_constraint)\n\n"

        # constructing writer constraint
        df['combined'] = "\"id\" == '" + df['writer_id'] + "'"

        writer_concat_string = ' or '.join(df['combined'])
        writer_constraint, writer_compound_result_or, _ = list_constraint(writer_concat_string, "%writer_constraint")
        cross_writers = "\n\n$crossnamespace_writers = $base.getVertex(" + writer_compound_result_or + ")\n\n"

        # constructing reader constraint
        reader_df = dict_dfs_readers[entity_tuple]
        print(reader_df)
        print("\n**********\n")
        reader_df['combined'] = "\"id\" == '" + reader_df['id'] + "'"
        print(reader_df['combined'])

        reader_concat_string = ' or '.join(reader_df['combined'])
        #print("\n**********\n")
        #print(reader_concat_string)
        reader_constraint, reader_compound_result_or, _ = list_constraint(reader_concat_string, "%reader_constraint")
        cross_readers = "\n\n$crossnamespace_readers = $base.getVertex(" + reader_compound_result_or + ")\n\n"

        # constructing output svg path, svg dump command, and reset workspace command
        svg_name = "\n\nexport > /home/vagrant/transformed_graph/" + entity_tuple[0] + "_" + entity_tuple[1][3:] + "_" + entity_tuple[2] + "_graph.json"
        svg_dump = "\n\ndump all $transformed_subgraph"
        reset_workspace = "\n\n########## Graph number: " + str(counter) + " ##########\n\nreset workspace\n\n"


        with open("individual_graph_query", 'a') as f:
            f.write(reset_workspace)

            f.write(entity_constraint)
            f.write(cross_entities)

            f.write(reader_constraint)
            f.write(cross_readers)

            f.write(writer_constraint)
            f.write(cross_writers)

            f.write(query_template)

            f.write(svg_name)
            f.write(svg_dump)
    #
    '''




if __name__ == "__main__":
    try:
        print("Starting program...")
        main()
    except KeyboardInterrupt:
        print("Error. Shutting down...")
