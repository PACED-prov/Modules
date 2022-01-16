from json import loads, dumps, dump
from os import write
from queue import PriorityQueue

pq = PriorityQueue()
def isVertex(obj):

    if (obj["type"] == "Entity") or (obj["type"] == "Activity"):
        return True

    return False

def extractRelationID(obj):

    try:
        return int(obj["annotations"]["relation_id"])
    except:
        pass

    return None

# function to read log at a given path
def readLog(path):
    # return open(path, 'r').readlines()
    output_file = open("./sorted_social_network_socfb_2.log", "a")
    myfile = open(path, 'r')
    print("reading first line")
    myline = myfile.readline()
    # counter = 0
    while myline:
        try:
            obj = loads(myline)
            if isVertex(obj): 
                # dump(obj, output_file)
                output_file.write(myline)
            else:
                relation_id = extractRelationID(obj)
                pq.put((relation_id, myline))
        except:
            print("BLEKH") 
            print(myline)  

        myline = myfile.readline()
        # counter = counter + 1
        # if counter == 4:
        #     break
    output_file.close()
    myfile.close()



def checkFromAnnot(obj):
    try:
        if obj["from"]:
            return True
    except:
        pass

    return False



def main(logpath):
    # reading log
    # lines = readLog(logpath)
    # print("Done Reading")

    # # print(lines[0])
    # # vertices = []
    # edges = PriorityQueue()

    # # typesss = []
    # print("here123")


    # print("Going into writing onto the file")

    # for i in range(0, 3):
    #     print("iteration number:", i)
    #     obj = loads(lines[i])
    #     print("\nloaded")
    #     dumped = dumps(obj)
    #     print("\ndumped")
    #     output_file.write(dumped)
    #     output_file.write('\n')
    #     print("\nwritten")

    readLog(logpath)
    print("file reading done")

    output_file = open("./sorted_social_network_socfb_2.log", "a")

    print("extracting pq starting")

    while not pq.empty():
        (_, line) = pq.get()
        output_file.write(line)
    output_file.close()

    print("extracting pq stopped")








main("./social_network_socfb_2.log")