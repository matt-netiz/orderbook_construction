
import os
import sys
import traceback
import time
from extract import *

if __name__ == '__main__':
    input_folder = os.path.join(sys.path[0], 'input')
    output_folder = os.path.join(sys.path[0], 'output')
    for filename in os.listdir(input_folder):
        try:
            name = filename[:-4]
            print(f"Extracting {name}.")
            start = time.time()
            result = extract_orderbooks(name, input_folder,output_folder)
            end = time.time()
            if result:
                sys.exit(f"Extraction of {name} done in {int(end - start)} seconds")
            else:
                sys.exit(f"Extraction of {name} already done")
        except:
            print(traceback.format_exc())
