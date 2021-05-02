import sys
import os
import math
import operator
from itertools import islice
from datetime import datetime
import copy
import heapq
from threading import Thread
startTime = datetime.now()
metafile='files/metadata.txt'


class Element(object):
    def __init__(self, record,index):
        self.record = record
        self.index = index
    def __lt__(self, other):
        return compare(self.record,other.record)

def error(msg):
    print(msg)
    exit(0)

def compare(lst1,lst2):
    for i in SORT_INDEX:
        if REVERSE:
            return (lst1[i]!=lst2[i] and lst1[i]>lst2[i])
        else:
            return (lst1[i]!=lst2[i] and lst1[i]<lst2[i])
    return False

def get_file_size(filename):
    f=open(filename,'r')
    line = f.readline()
    if line:
        record_size=(sys.getsizeof(line)-48)
        file_size=os.stat(filename).st_size
    if(file_size%record_size!=0):
        error("file size is not multiple of record size")
    return [file_size,record_size]

def get_meta_data(filename):
    try:
        with open(filename,'r') as f:
            lines = f.readlines()
            names=[]
            sizes=[]
            for line in lines:
                if line=="\n":
                    break
                name,size=line.split(',')
                names.append(name)
                sizes.append(int(size))
    except:
        error("Couldn't open file: "+filename)
    return [names,sizes]

def subscript(col):
    val=COL_NAMES.index(col)
    return "x["+str(val)+"]"

def initialization(args):
    # print(args)
    if(len(args)<6):
        error("Incorrect number of arguments")
    global FILE_SIZE
    global RECORD_SIZE
    global COL_NAMES
    global COL_LEN
    global MAIN_MEM
    global NUM_REC
    global REVERSE
    # global SORT_COLS
    global SORT_INDEX
    global NUMBER_OF_THREADS
    global FLAG
    FLAG = False
    COL_NAMES,COL_LEN = get_meta_data(metafile)
    FILE_SIZE,RECORD_SIZE=get_file_size(args[1])
    NUM_REC = FILE_SIZE /RECORD_SIZE
    try:
        MAIN_MEM = int(args[3])*1024*1024
    except:
        error("Incorrect arguments: check main memory size")
    #number of records that can fit in main memory
    MAIN_MEM = math.floor(MAIN_MEM/RECORD_SIZE)
    ind=4
    if args[ind].lower() != "asc" and args[ind].lower() != "desc":
        ind +=1
        FLAG = True
        NUMBER_OF_THREADS = int(args[4])
    if args[ind].lower()=="asc":
        REVERSE=False
    elif args[ind].lower()=="desc":
        REVERSE=True
    else:
        error("Incorrect arguments")
    ind +=1    
    cols=args[ind:]
    # SORT_COLS=[]
    SORT_INDEX=[]
    for col in cols:
        if(col not in COL_NAMES):
            error("sorting column "+ col +" doesn't exist")
        # SORT_COLS.append(subscript(col))
        SORT_INDEX.append(COL_NAMES.index(col))
    # SORT_COLS=(','.join(SORT_COLS)).strip()
    
    print("file size: ",FILE_SIZE)
    print("record size: ", RECORD_SIZE)
    print("column names: ",COL_NAMES)
    print("column lens: ",COL_LEN)
    print("main mem: ",MAIN_MEM)
    print("total number of records: ", NUM_REC)
    print("sort colums: ",SORT_INDEX)

def get_record(line):
    lst=[]
    start=0
    for i in range(len(COL_LEN)):
        lst.append(line[start:start+COL_LEN[i]])
        start=start+COL_LEN[i]+2
    return lst

# def get_record1(line):
#     lst=line.split('  ')
#     try:
#         if lst[-1][-1] == '\n':
#             lst[-1]=lst[-1][0:-1] 
#     except:
#         print(lst)
#     return lst

def write_records(filename,records):
    try:
        f=open(filename,'w')
    except:
        error("Error: write_records() - error in opening file: "+filename)
    for record in records:
        line="  ".join(record)
        line=line+"\n"
        f.write(line)
    f.close()

def reading_without_threads(filename,size,n):
    num_lines=int(min(size,NUM_REC-n*size))
    try:
        f= open(filename, "r")
    except :
        error("Error: read_records() - error in opening file: "+filename)
    f.seek((n*size)*RECORD_SIZE, 0)
    lines=islice(f, num_lines)
    records=[]
    for line in lines:
        records.append(get_record(line))
    f.close()
    return records

def threading_function(filename,size,num_lines,result,j,n):
    f= open(filename, "r")
    f.seek((n+ j*size)*RECORD_SIZE, 0)
    lines=islice(f, num_lines)
    records=[]
    for line in lines:
        records.append(get_record(line))
    print("thread",j," completed reading ",len(records))
    result[j]=records
    f.close()

def reading_with_threads(filename,sublist_size,i):
    num_lines=int(min(sublist_size,NUM_REC-i*sublist_size))
    n=i*sublist_size
    size= math.ceil(num_lines/NUMBER_OF_THREADS)
    threads = [None] * NUMBER_OF_THREADS
    result=[None] * NUMBER_OF_THREADS
    
    for j in range(NUMBER_OF_THREADS):
        lines= int(min(size,num_lines-j*size))
        if lines>0:
            threads[j] = Thread(target=threading_function,args=(filename,size,copy.deepcopy(lines),result,copy.deepcopy(j),i*sublist_size))
            threads[j].start()
    records=[]
    for j in range(len(threads)):
        threads[j].join()
    for j in range(NUMBER_OF_THREADS):
        if result[j] is not None:
            records=records+result[j]
    return records

def read_records(filename,size,i,threads=False):
    if threads:
        return reading_with_threads(filename,size,i)
    else:
        return reading_without_threads(filename,size,i)

def phase2(filename):
    global phase2_startTime
    phase2_startTime = datetime.now()
    print("\n##running Phase-2")
    temp_files = [None]*NUM_SUBLISTS
    try:
        output_file = open(filename,'w')
    except:
        error("Error: Phase2- couldn't open the file")
    for i in range(NUM_SUBLISTS):
        name="temp_"+str(i)
        temp_files[i]=open(name,"r")
    
    # file1.readline()  
    pq = []
    for i in range(NUM_SUBLISTS):
        line = temp_files[i].readline()
        if len(line)>0:
            ele = Element(get_record(line),i)
            heapq.heappush(pq,ele)
    while(len(pq)>0):
        ele = heapq.heappop(pq)
        i=ele.index
        line="  ".join(ele.record) + "\n"
        output_file.write(line)
        line = temp_files[i].readline()
        if not line:
            temp_files[i].close()
            os.remove("temp_"+str(i))
        else:
            ele = Element(get_record(line),i)
            heapq.heappush(pq,ele)

def phase1(filename):
    global phase1_startTime 
    global NUM_SUBLISTS
    phase1_startTime = datetime.now() 
    sublist_size = MAIN_MEM
    NUM_SUBLISTS=math.ceil(NUM_REC/sublist_size)
    if(NUM_SUBLISTS >= MAIN_MEM):
        error("Error: Not possible to sort using 2 Phase merge sort. Require more than thwo phases.")
    print("\n##running Phase-1") 
    print("\nNumber of sublists: "+str(NUM_SUBLISTS))
    for i in range(NUM_SUBLISTS):
        records= read_records(filename,sublist_size,i,threads=FLAG)
        print("\nsorting sublist #"+ str(i+1)+"..")
        records.sort(key=operator.itemgetter(*SORT_INDEX), reverse=REVERSE)
        # records.sort(key = lambda x:(eval(SORT_COLS)),reverse=REVERSE )
        print("\nwriting to disk #"+ str(i+1)+"..")
        print("    ....")
        intermediate_file="temp_"+str(i)
        write_records(intermediate_file,records)

def main():
    print("###start execution")
    initialization(sys.argv)
    input_file=sys.argv[1]
    output_file=sys.argv[2]
    phase1(input_file)
    global phase1_time
    phase1_time=str(datetime.now() - phase1_startTime)
    phase2(output_file)

if __name__ == "__main__":
    main()
    print("Total time taken : ",str(datetime.now() - startTime))
    print("Time taken for Phase1 :",phase1_time)
    print("time taken for Phase2 : ",str(datetime.now() - phase2_startTime ))


# def get_column_size(lines):
#     cols=lines[9].split("  ")
#     if cols[-1][-1] == '\n':
#         cols[-1]=cols[-1][0:-1]
#     lst=[]
#     for col in cols:
#         lst.append(sys.getsizeof(col)-49)
#     print(lst)