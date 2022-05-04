import os
import sys
import threading


class ProgressPercentage(object):
    """ 
    Script will track the uploading progress of each data file being transferred to cloud data storage.
    
    """
    
    def __init__(self, file_path):
        """
        Args: 
            file_path (str): Data file's full directory path (incl. filename).
            
        """
        
        # Data file's full directory path (incl. filename).
        self.file_path = file_path
        
        # File's size. Note: Calculated via powers of 1000 rather than 1024.
        self.size = float(os.path.getsize(file_path))
        
        # Initialize filesize counter, which details the file size that has already been
        # uploaded at any given time.
        self.seen_so_far = 0
        
        # Lock worker threads to prevent losing the worker threads during file processing.
        self.lock = threading.Lock()
            
    def __call__(self, bytes_amount):
        """
        Establish an uploading progress bar.
        
        Args:
            bytes_amount (float): Bytes that have already been transferred to the cloud data storage
                                  for the given file.
                                  
        Return: None

        """
        
        # Once worker threads are locked, cumulate filesize counter.
        with self.lock:
            
            # Cumulative file sizes in bytes. Note: Calculated via powers of 1000 rather than 1024.
            self.seen_so_far += bytes_amount
            
            # Percentage of the file uploading progress.
            percentage = (self.seen_so_far / self.size) * 100
            
            # Display progress bar.
            sys.stdout.write("\r%s  %s / %s  (%.2f%%)" % (self.file_path, self.seen_so_far, self.size, percentage))
            
            # Return system resource back to memory.
            sys.stdout.flush()
        
        return