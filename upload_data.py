# Create S3 resource to connect to S3 via SDK
import boto3
from boto3.s3.transfer import TransferConfig
import botocore
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import time
from progress_bar import ProgressPercentage
session = boto3.Session(profile_name='srw-app')
s3 = session.resource('s3')


class UploadData():
    """
    Upload datasets of interest to cloud data storage.
    
    """
    def __init__(self, file_relative_dirs, use_bucket):
        """
        Args: 
            file_relative_dirs (list): List of relative directory paths on-prem to obtain 
                                       the dataset files.
            use_bucket (str): If set to 'rt', datasets will be uploaded to the cloud data
                              storage bucket designated for the UFS RT datasets. Set to 'srw'
                              if datasets will be uploaded to the cloud data
                              storage bucket designated for the UFS SRW datasets.
                              
        """
        
        # Main on-prem directory to locate the datasets. 
        self.work_dir = './'
        
        # List of data files' relative directory paths on-prem. 
        self.file_relative_dirs = file_relative_dirs
        
        if use_bucket == 'rt':
            self.bucket_name = 'noaa-ufs-regtests-pds'
        elif use_bucket == 'srw':
            self.bucket_name = 'noaa-ufs-srw-pds'
        else:
            print(f"{use_bucket} Bucket Does Not Exist.")

    def upload_single_file(self, file_dir):
        """
        Upload a single data file to cloud w/ an established API configuraton.

        Args:
            file_dir (str): Relative directory path of the data file to transfer to 
                            cloud data storage.
            
        Return: None
        
        The AWS SDK uploader will manage data file retries and handle multipart as well as 
        non-multipart data transfers. To retain the current dataset
        directory paths established on the RDPHPCS, Orion, each key of a data 
        file object will be set to their source directory path as designated on Orion. 
        Reason: Configured as sch to avoid altering too many variables within the 
        UFS-WM Regression Test scripts for when the UFS-WM Regression Test framework 
        begins to establish an experimental directory for the transferring of UFS data
        files from and to the RDHPCS on-prem disk and user's experimental directory, respectively.
        
        Note: The callback script, ProgressPercentage, will calculate each file size in powers
        of 1000 rather than 1024. The file size displayed from S3 cloud storage bucket will 
        calculate each file size in powers of 1024.
        
        **TODO** If utilizing Jupyter Notebook, set NotebookApp.iopub_data_rate_limit=1.0e10 w/in 
        the configuration file: "/.jupyter/jupyter_notebook_config.py"

        """

        # Configuration for multipart upload.
        KB, MB, GB = 1024, 1024**2, 1024**3

        start_time = time.time()
        config = TransferConfig(multipart_threshold=100*MB,
                                max_concurrency=10,
                                multipart_chunksize=50000*KB,
                                num_download_attempts=2,
                                use_threads=True)

        # Upload file w/out extra arguments.
        # Track multi-part upload progress current percentage, total, remaining size, etc
        key_path = file_dir
        s3.meta.client.upload_file(self.work_dir + file_dir,
                                   self.bucket_name,
                                   key_path,
                                   Config=config,
                                   Callback=ProgressPercentage(self.work_dir + file_dir))
        
        # Upload file w/ extra arguments.
        #s3.meta.client.upload_file(self.work_dir + file_dir,
        #                           self.bucket_name,
        #                           key_path, 
        #                           ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/nc'},
        #                           Config=config,
        #                           Callback=ProgressPercentage(self.work_dir + file_dir))
        
        end_time = time.time()
        
        # Processing time to upload file.
        delta = (end_time-start_time)/60
        print(f'Processing Time (min): {delta}\n')

        return 
    
    def upload_files2cloud(self):
        """
        Iterates through the list of data files' relative directory paths on-prem. 

        Args:
            None
            
        Return: None
        
        *Note: For UFS-WM RT will be keeping 'INPUTDATA_ROOT_WW3' as a key within
        the mapped dictionary -- in case, the NOAA development team decides 
        restructure & migrate 'WW3_input_data_YYYYMMDD' out of the 
        'input-data-YYYYMMDD' folder then, will need to track the 'INPUTDATA_ROOT_WW3'
        related data files.
        
        """
        for dataset_type, ts_files in self.file_relative_dirs.items():
            for file_dir in ts_files:
                self.upload_single_file(file_dir)
                
        return 
    
    def multi_part_upload_with_s3_withTuning(self, file_dir, chunk_sz_list):
        """
        Tuning API parameters for uploading a single data file to cloud data storage.

        Args:
            file_dir (str): Directory path of the file to transfer to cloud.
            chunk_sz_list (list): List of the range of partition sizes to perform
                                  multi-upload data transferrring.
            
        Return (pd.DataFrame): The amount of time it takes to transfer a given data file 
        versus the set chunksize.
        
        Used to configure the following API parameters -- in an effort to improve the uploading
        performance of the UFS datasets to cloud. Note: The AWS SDK uploader will manage data
        file retries and handle multipart as well as non-multipart data transfers.

        API Parameters:
        - __multipart_threshold:__ Transfer size threshold for which multipart uploads, downloads, 
        and copies will be automatically triggered against a given data file. Ensure multipart 
        uploads/downloads only happen if the size of a transfer is larger than the set 
        'multipart_threshold.'

        - __max_concurrency:__ Maximum number of threads that will be making requests to perform
        a data transfer. If 'use_threads' is set to False, the 'max_concurrency' value is ignored
        since, the data transfer would then be set to using the single main thread.

        - __multipart_chunksize:__ Partition size of each part of the data file when a multipart
        transfer is being performed.

        - __num_download_attempts:__ Number of download attempts retried upon errors when
        downloading an object from the cloud data storage bucket. Note: These retries account for 
        errors for which occur when streaming data down from the cloud data storage such as 
        socket errors and read timeouts that may occur after receiving an 'OK' response from the cloud data
        storage bucket. Exceptions such as throttling errors and 5xx errors are already
        retried by botocore (default=5). The 'num_download_attempts' does not take into account the
        number of exceptions retried by botocore.

        - __max_io_queue:__ Maximum amount of read parts that can be queued in-memory to be written for a
        download. The size of each of these read parts is at most the size of the 'io_chunksize.'
        
        - __io_chunksize:__ Maximum size of each chunk in the I/O queue.

        - __use_threads:__ If set to True, worker threads will be used when performing S3 transfers. 
        If set to False, no additional worker threads will be used and data transfers will be be ran
        via the single main thread.

        - __max_bandwidth:__ Maximum bandwidth (int; bytes per second) that will be consumed in uploading
        and downloading the file content.

        """
        
        #[For Tuning Configuring API Parameters].
        #chunk_sz_list = [50000] #list(range(40000, 61000, 500)) # In KB

        # Configuration API multipart upload.
        KB, MB, GB = 1024, 1024**2, 1024**3
        proc_time_list = []
        for chunk_sz in chunk_sz_list:
            print(f'Chunk Size: {chunk_sz}\n')
            start_time = time.time()
            config = TransferConfig(multipart_threshold=100*MB,
                                    max_concurrency=10,
                                    multipart_chunksize=chunk_sz*KB,
                                    num_download_attempts=2,
                                    use_threads=True)


            # Upload a file w/out extra arguments
            # Track multi-part upload progress current percentage, total, remaining size, etc
            key_path=file_dir
            s3.meta.client.upload_file(self.work_dir + file_dir,
                                       self.bucket_name,
                                       key_path,
                                       Config=config,
                                       Callback=ProgressPercentage(self.work_dir + file_dir))
            end_time = time.time()
            
            # Processing time to upload file.
            delta = (end_time-start_time)/60
            print(f'Processing Time (min): {delta}\n')
            proc_time_list.append(delta)
        
        # Log processing time to upload file and the corespond. set data partition size.
        time2chunksz_df = pd.DataFrame([chunk_sz_list, proc_time_list], index=['chunk_sz', 'xfer_time']).T

        return time2chunksz_df
    
    def purge(self, key_path):
        """
        Remove data file object w/ the given key from cloud data storage.
        
        Args:
            key_path (str): Key of the data file object w/in the cloud data storage.
            
        Return: None

        """
        s3.Object(self.bucket_name, key_path).delete()

        return

    def purge_by_keyprefix(self, key_prefix):
        """
        Remove data file object w/ the given key prefix from cloud data storage.
        
        Args:
            key_prefix (str): Key's prefix of the data objects to delete w/in
                              cloud data storage.
            
        Return: None

        """
        
        objects = s3.Bucket(self.bucket_name).objects.filter(Prefix=key_prefix)
        for ob in objects:
            print(ob)
        objects.delete()
        print(f"\nCompleted: {key_prefix} prefixed Objects have been deleted")
        
        return
    
    def get_all_s3_keys(self):
        """
        Extract all data file object keys w/ from cloud data storage.
        
        Args:
            None
            
        Return (list): List of all objects within the bucket of interest.

        """
        
        # Instantiate bucket of interest.
        bucket_ob = s3.Bucket(self.bucket_name)
        keys = []
        for obj in bucket_ob.objects.all():
            keys.append(obj.key)
            
        return keys
    
    def rename_s3_keys(self, source_key_path, new_key_path):
        """
        'Rename' an existing object's key.
        
        Args:
            source_key_path (str): Key of the existing object.
            new_key_path (str): New key to set for the existing object.
            
        Return (list): List of all objects within the bucket of interest.

        """
            
        # Set original object with new object key.
        s3.Object(self.bucket_name, new_key_path).copy_from(CopySource=source_key_path)
        
        # Delete the orginal object.
        s3.Object(self.bucket_name, source_key_path).delete()
        print(f"The object's key, {source_key_path}, has been renamed to: {new_key_path}")
            
        return