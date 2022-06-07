import pandas as pd
import numpy as np
import json
import sys


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
    # reading filter output json file
    df = pd.read_json("./28_recent.json", lines=True)

    # loading static query template
    query_template = ""

    with open('./query_template', 'r') as f:
        lines = f.readlines()
        query_template = "".join(lines)

    #loading and pre_parsing 
    df_artifact = pd.json_normalize(df['artifact'])
    column_dict = {'subtype':'artifact.subtype', 'tgid':'artifact.tgid', 'read fd': 'artifact.read_fd', 'write fd':'artifact.write fd', 'inode':'artifact.inode', 'fd':'artifact.fd', 'fd 1':'artifact.fd_1', 'fd 0':'artifact.fd_0', 'local address':'artifact.local_address', 'remote port':'artifact.remote_port', 'protocol':'artifact.protocol', 'remote address':'artifact.remote_address', 'local port':'artifact.local_port'}
    df_artifact.rename(columns = column_dict , inplace = True)
    # df_artifact= df_artifact[['artifact.inode', 'artifact.subtype','artifact.local_address', 'artifact.remote_port','artifact.protocol','artifact.remote_address','artifact.local_port', 'artifact.tgid','artifact.read_fd', 'artifact.write fd', 'artifact.fd', 'artifact.fd_1', 'artifact.fd_0']]

    #For Readers
    df_reader = pd.json_normalize(df['reader'])
    df_reader = df_reader.add_prefix('reader.')
    df_artifact_reader = pd.concat([df_artifact, df_reader], axis=1)
    df_artifact_reader = df_artifact_reader[(df_artifact_reader['artifact.subtype'] == 'file') | (df_artifact_reader['artifact.subtype'] == 'directory')]
    df_artifact_reader = df_artifact_reader.drop_duplicates()
    df_artifact_reader = df_artifact_reader[df_artifact_reader['reader.user namespace'] != '-1']

    cols = df_artifact.columns
    cols = list(cols)
    df6 = df_artifact_reader.set_index(cols).sort_index()
    uni = df6.index.unique()
    list_dfs = []

    reader_constraint_list = {}
    inodes = []
    for i in range(len(uni)):
        dfx = df6.loc[uni[i]].reset_index()
        inodes.append(dfx['artifact.inode'][0])
        reader_start = dfx[['reader.start time', 'reader.pid']].dropna().drop_duplicates()
        reader_start['constraint'] = "(\"start time\" == '" + reader_start['reader.start time']+ "'" + " and " + "\"pid\" == '" + reader_start['reader.pid']+ "'" + ")" 
        # reader_seen = dfx[['reader.seen time', 'reader.pid']].dropna().drop_duplicates()
        # reader_seen['constraint'] = "(\"seen time\" == '" + reader_seen['reader.seen time']+ "'" + " and " + "\"pid\" == '" + reader_seen['reader.pid']+ "'" + ")" 
        # constraint_seen = ' or '.join(reader_seen['constraint'].drop_duplicates())
        constraint_seen = ''
        reader_seen = ''
        constraint_start = ' or '.join(reader_start['constraint'].drop_duplicates())
        if(constraint_seen == ''):
            reader_constraint = constraint_start
        elif(constraint_start == ''):
            reader_constraint = constraint_seen
        else:
            reader_constraint = constraint_seen + ' or ' + constraint_start
        # reader_constraint_list.append(reader_constraint)
        reader_constraint_list[dfx['artifact.inode'][0]] = reader_constraint
        list_dfs.append(dfx)
    
    #For Writers
    dfs = []
    for i in range(df.shape[0]):
        new_df = pd.json_normalize(df.iloc[i]['writers'])
        new_df['index'] = i
        dfs.append(new_df)
    writer_df = pd.concat(dfs).set_index('index')
    writer_df = writer_df.add_prefix('writer.')
    merged = pd.merge(df_artifact, writer_df, left_index=True, right_index=True)
    merged = merged.drop_duplicates()
    merged = merged [(merged['artifact.subtype'] == 'file') | (merged['artifact.subtype'] == 'directory')]
    merged = merged[merged['writer.user namespace'] != '-1']
    cols = df_artifact.columns
    cols = list(cols)
    df6 = merged.set_index(cols).sort_index()
    uni = df6.index.unique()
    list_dfs = []
    writer_inodes = []
    writer_constraint_list = {}
    for i in range(len(uni)):
        
        dfx = df6.loc[uni[i]].reset_index()
        #will continue if writer exists but reader does not exist
        if(dfx['artifact.inode'][0] not in inodes):
            continue
        else:
            writer_inodes.append(dfx['artifact.inode'][0])

        #will go into except if reader exists but writer namespaces are all -1 and have been dropped 

        writer_start = dfx[['writer.start time', 'writer.pid']].dropna().drop_duplicates()
        writer_start['constraint'] = "(\"start time\" == '" + writer_start['writer.start time']+ "'" + " and " + "\"pid\" == '" + writer_start['writer.pid']+ "'" + ")" 
        # writer_seen = dfx[['writer.seen time', 'writer.pid']].dropna().drop_duplicates()
        # writer_seen['constraint'] = "(\"seen time\" == '" + writer_seen['writer.seen time']+ "'" + " and " + "\"pid\" == '" + writer_seen['writer.pid']+ "'" + ")" 
        # constraint_seen = ' or '.join(writer_seen['constraint'].drop_duplicates())
        writer_seen = ''
        constraint_seen = ''
        constraint_start = ' or '.join(writer_start['constraint'].drop_duplicates())
        if(constraint_seen == ''):
            writer_constraint = constraint_start
        elif(constraint_start == ''):
            writer_constraint = constraint_seen
        else:
            writer_constraint = constraint_seen + ' or ' + constraint_start
        writer_constraint_list[dfx['artifact.inode'][0]] = writer_constraint
        list_dfs.append(dfx)   

    inodes = writer_inodes

    for i in range(len(inodes)):
        artifact_constraint = "%artifact_constraint = \"inode\" == '" + str(inodes[i]) + "'\n" 
        cross_artifacts = "\n$crossnamespace_artifacts = $base.getVertex(%artifact_constraint)\n\n"

        writer_constraint, writer_compound_result_or, _ = list_constraint(writer_constraint_list[inodes[i]], "%writer_constraint")
        cross_writers = "\n\n$crossnamespace_writers = $base.getVertex(" + writer_compound_result_or + ")\n\n"

        reader_constraint, reader_compound_result_or, _ = list_constraint(reader_constraint_list[inodes[i]], "%reader_constraint")
        cross_readers = "\n\n$crossnamespace_readers = $base.getVertex(" + reader_compound_result_or + ")\n\n"
        
        # constructing output svg path, svg dump command, and reset workspace command
        svg_name = "\n\nexport > /home/vagrant/svg_dump/" + str(inodes[i]) + "_graph.dot"
        svg_dump = "\n\ndump all $subgraph"
        reset_workspace = "\n\n#################################\n\nreset workspace\n\n"

        with open("query", 'a') as f:
            f.write(reset_workspace)

            f.write(artifact_constraint)
            f.write(cross_artifacts)

            f.write(reader_constraint)
            f.write(cross_readers)

            f.write(writer_constraint)
            f.write(cross_writers)

            f.write(query_template)

            f.write(svg_name)
            f.write(svg_dump)
    
if __name__ == "__main__":
    try:
        print("Starting program...")
        main()
    except KeyboardInterrupt:
        print("Error. Shutting down...")
