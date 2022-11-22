from progress_bar import ProgressPercentage
from upload_data import UploadData
import sys

class TransferSrwTar():
    """
    Obtain directories for the datasets on-disk & migrate to SRW cloud storage.
    
    """
    def __init__(self, object_dir, key_path = None):
        """
        Upload a single data file to cloud w/ an established API configuraton.

        Args:
            file_dir (str): Relative directory path of the object (e.g. tar folder) on RDHPCS
                            to transfer to cloud data storage.
            key_path (str): Establish key for object in cloud. If None, the key
                            of the object will be set to the object's local folder 
                            directory location by default.
        """
        
        # Instantiate SRW uploader
        uploader_wrapper = UploadData(file_relative_dirs = None, use_bucket = 'srw')
        
        # Migrate object to SRW cloud bucket
        uploader_wrapper.upload_single_srw_folder(object_dir, key_path)
   
if __name__ == '__main__':
    
    # Migrate object to SRW cloud bucket
    TransferSrwTar(sys.argv[1], sys.argv[2])
    

