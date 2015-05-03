# -*- coding: utf-8 -*-
"""
Copyright (c) 2015 by Patrick Hall, jpatrickhall@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-------------------------------------------------------------------------------

Stub with simple example for using Python multiprocessing to complete
independent tasks.

N_THREAD: Number of processes to use; the script will create this many
          chunks and working sub-directores.

WORKING_DIR: Parent directory in which the sub-directories for each
             chunk will be created.

INPUT_DATA: File to split into chunks and process.

Run by setting constants here OR by command line:
run_on_chunks.py -f <input file> -p <number of processes> -d <output directory>

"""

import getopt
import math
import multiprocessing
import os
import sys
import time
from multiprocessing import Process

# TODO: Set constants
N_THREAD = 4
WORKING_DIR = 'C:\Temp'
INPUT_DATA = 'sample_data.txt'

def create_out_dirs():

    """ Creates N_THREAD number of output directories.

    Raises:
        EnvironemtError: Problem creating directories.
    """

    print '-------------------------------------------------------------------'
    print 'Creating working directory structure ... '

    for i in range(0, int(N_THREAD)):

        chunk_outdir = WORKING_DIR + os.sep + '_chunk_dir' + str(i)
        try:
            if not os.path.exists(chunk_outdir):
                os.mkdir(chunk_outdir)
                print 'Created ' + chunk_outdir + ' ...'
        except EnvironmentError as exception_:
            print exception_.message
            print 'Failed to locate or create ' + chunk_outdir + '!'
            sys.exit(-2)

    print 'Done.'

def chunk_files():

    """ Separates INPUT_DATA into N_THREAD roughly equal chunks.

    Each in a separate directory (created in create_out_dirs) for thread
    safety.

    """

    print '-------------------------------------------------------------------'
    print 'Chunking ' + INPUT_DATA + ' ...'

    nline = sum(1 for line in open(INPUT_DATA))
    chunk_size = math.floor(nline/int(N_THREAD))

    n_thread = int(N_THREAD)
    j = 0
    with open(INPUT_DATA) as file_:
        for i, line in enumerate(file_):
            if (i + 1 == j * chunk_size and j != n_thread) or i == nline:
                out.close()
            if i + 1 == 1 or (j != n_thread and i + 1 == j * chunk_size):
                chunk_outdir = WORKING_DIR + os.sep + '_chunk_dir'\
                    + str(j)
                chunk_file = chunk_outdir + os.sep + '_raw' + str(j) + '.txt'
                if os.path.isfile(chunk_file):
                    break
                out = open(chunk_file, 'w+')
                j = j + 1
            if out.closed != True:
                out.write(line)
            if i % 1000 == 0 and i != 0:
                print 'Processing line %i ...' % (i)

        print 'Done.'

def some_task(i, in_prefix='_raw'):

    """ Some task that executes on each chunk independently.

    Args:
        i: Thread index.
        in_prefix: The filename prefix for locating input files; can be
            changed to alter the order of execution.

    """

    chunk_dir = WORKING_DIR + os.sep + '_chunk_dir' + str(i)
    in_file = chunk_dir + os.sep + in_prefix + str(i) + '.txt'
    out_file = chunk_dir + os.sep + in_prefix + '_some_task_completed' +\
        str(i)  + '.txt'

    process_name = multiprocessing.current_process().name

    out = open(out_file, 'wb')
    with open(in_file) as in_f:
        # TODO: your task here
        for j, line in enumerate(in_f):
            if j % 100 == 0:
                print process_name + ': Hello world ...'
            out.write(line)

def main(argv):
    """ For running standalone.

    Args:
        argv: Command line args.

    Raises:
        GetoptError: Problem parsing command line options.
        BaseException: Some problem from a task.
    """

    # Init vars
    n_thread = None
    input_data = None
    working_dir = None

    global N_THREAD
    global INPUT_DATA
    global WORKING_DIR

    # Parse command line args
    try:
        opts, _ = getopt.getopt(argv, "f:d:p:h")
        for opt, arg in opts:
            if opt == '-d':
                working_dir = arg
            elif opt == '-f':
                input_data = arg
            elif opt == '-p':
                n_thread = arg
            elif opt == '-h':
                print 'run_on_chunks.py -f <input file> -p <number of processes> -d <output directory>'
                sys.exit(0)
    except getopt.GetoptError:
        print 'run_on_chunks.py -f <input file> -p <number of processes> -d <output directory>'
        sys.exit(-1)

    # Apply command line args
    if n_thread != None:
        N_THREAD = n_thread
    if input_data != None:
        INPUT_DATA = input_data
    if working_dir != None:
        WORKING_DIR = working_dir

    print '-------------------------------------------------------------------'
    print 'Proceeding with options: '
    print 'Processes (-p)           = %s' % (N_THREAD)
    print 'Output directory (-d)    = %s' % (WORKING_DIR)
    print 'Input file (-f)          = %s' % (INPUT_DATA)

    # Init chunk directory structure and chunks
    create_out_dirs()
    chunk_files()

    # Multiprocessing scheme to execute tasks on chunks
    # You may have as many tasks as you like

    print '-------------------------------------------------------------------'
    print 'Executing some task on chunks ... '
    tic = time.time()
    processes = []
    try:
        for i in range(0, int(N_THREAD)):
            process_name = 'Process_' + str(i)
            process = Process(target=some_task, name=process_name, args=(i,))
            process.start()
            processes.append(process)
        for process_ in processes:
            process_.join()
        print 'Completed some task in %.2f s.' % (time.time()-tic)
    except BaseException as exception_:
        print exception_.message
        print 'ERROR: Could not complete task 1.'
        print sys.exc_info()
        exit(-1)

if __name__ == '__main__':
    main(sys.argv[1:])
