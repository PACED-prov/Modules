from json import loads
from queue import PriorityQueue
import sys
import os

'''
 --------------------------------------------------------------------------------
 @What it does?
    The following Python module is designed to ingest a CamFlow log and sort it
    in a way that all vertices are piled up at the top followed by all the edges
    sorted in a sequential order based on their 'relation_id'. 
 
 @When should you use it?
    If there is a strict need in a module that requires a vertex to be present
    before the edge of that vertex is encountered in a log then this Python 
    module can help you bring your CamFlow log in a desired shape. 
 
 @authors Shahpar Khan, Mashal Abbas
 --------------------------------------------------------------------------------
'''


# Function that returns True if the current JSON object is a vertex --- False if it is an edge
def isVertex(obj):

    if (obj["type"] == "Entity") or (obj["type"] == "Activity"):
        return True

    return False


# Function that extract relation_ids from the JSON object
def extractRelationID(obj):

    try:
        return int(obj["annotations"]["relation_id"])
    except:
        print("Error in reading relation_id of the following JSON object:")
        print(obj)

    return None


# Function to dump all the edges in priority queue based on the relation_ids
def dumpPriorityQueue(pq, output_file):

    while not pq.empty():
        (_, line) = pq.get()
        output_file.write(line)


# Function to read log at a given path 
def readWriteLog(input_log_path, output_log_name):

    # Opening files
    try:
        input_file = open(input_log_path, 'r')
    except:
        print("Error in opening file at path:", input_log_path)

    try:
        output_file = open(output_log_name, "a")
    except:
        print("Error in opening file with name:", output_log_name) 
    
    # Initializing priority queue
    pq = PriorityQueue()

    line = input_file.readline()
    while line:
        try:
            obj = loads(line)
            if isVertex(obj): 
                output_file.write(line)
            else:
                relation_id = extractRelationID(obj)
                pq.put((relation_id, line))
        except:
            print("Error in ingesting the following line:") 
            print(line)  

        line = input_file.readline()

    dumpPriorityQueue(pq, output_file)

    output_file.close()
    input_file.close()


def main(input_log_path, output_log_name):
    
    readWriteLog(input_log_path, output_log_name)
    print("Done...")


if __name__ == '__main__':
    try:
        if len(sys.argv) != 3:
            raise Exception("run python3 sortlog.py <input_log_path> <output_log_name>")
        else:
            print("Starting...")
            print("Input log path:", sys.argv[1])
            print("Output log name:", sys.argv[2])
            main(sys.argv[1], sys.argv[2])
        
    except KeyboardInterrupt:
        print("Exiting...")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)