import os 
import pickle
from collections import defaultdict
import subprocess
import tarfile


class GetSrwData():
    """
    Extract locality of the UFS datasets of interest & generate a dictionary which will
    map the UFS dataset files into the following dataset types:
    Input data, WW3 input data, Baseline data, and BMIC data. 
    
    """
    
    def __init__(self, avoid_ma_fldrs, avoid_fix_fldrs, avoid_ne_fldrs, avoid_fc_sample_fldrs, fix_data_dir, input_model_data_dir, ne_data_dir, fc_sample_data_dir):
        """
        Args: 
            avoid_ma_fldrs (str): Foldername to ignore within main input model data directory 
                                  of interest on-prem.
            avoid_fix_fldrs (str): Foldername to ignore within main fix data directory of interest 
                                  on-prem.
            avoid_ne_fldrs (str): Foldername to ignore within main natural earth data directory of interest 
                                  on-prem.
            avoid_fc_sample_fldrs (str): Foldername to ignore within main  forecast sample data directory of interest 
                                  on-prem.                                  
            fix_data_dir (str): Source directory of the fixed datasets.
            input_model_data_dir (str): Source directory of the input model datasets.
            ne_data_dir (str): Source directory of the natural earth datasets. 
            fc_sample_data_dir (str): Source directory of the natural earth datasets. 

        """
        # == Proposed setup to transfer SRW fix, input data, natural earth, & fc data samples while reserving the 
        # directory locations as shown in the RDHPCS Orion. ==
        # Data sources.
        self.fix_data_dir = fix_data_dir
        self.input_model_data_dir = input_model_data_dir
        self.ne_data_dir = ne_data_dir
        self.fc_sample_data_dir = fc_sample_data_dir
        
        # Extract all data directories residing w/in datasets' main hpc directories.
        self.ma_file_dirs = self.get_data_dirs('input_model_data')
        self.fix_file_dirs = self.get_data_dirs('fix_data')
        self.ne_dirs = self.get_data_dirs('ne_data')
        self.fc_sample_dirs = self.get_data_dirs('fc_sample_data')
        
        # Remove file directories comprise of a folder name.        
        self.avoid_ma_fldrs = avoid_ma_fldrs
        self.avoid_fix_fldrs = avoid_fix_fldrs
        self.avoid_ne_fldrs = avoid_ne_fldrs
        self.avoid_fc_sample_fldrs = avoid_fc_sample_fldrs
        
        # List of all model analysis data files for SRW's multi-preprocessor.
        self.partition_ma_datasets = self.get_model_analysis_data()

        # Data files pertaining to specific timestamps of interest.
        # Select timestamp dataset(s) to transfer from RDHPCS on-disk to cloud.
        #self.filter2specific_ma_datasets = self.get_specific_model_analysis_files()

        # List of all grid fixed data files for SRW's multi-preprocessor.
        self.partition_fixed_datasets = self.get_fixed_data()
        
        # List of all natural earth files.
        self.partition_ne_datasets = self.get_ne_data()
        
        # List of all natural earth files.
        self.partition_fc_datasets = self.get_fc_data()
        
        # Requested by AUS to transfer SRW fix, input & natural earth data as tar objects.
        # List all SRW data directories from sources (filtered). 
        self.ma_data_list = self.get_tar_data_dirs('input_model_data')
        self.fix_data_list = self.get_tar_data_dirs('fix_data')
        self.ne_data_list = self.get_data_dirs('ne_data')         
        
        # TODO: Adding SRW forecast samples to support SRW application (include: Observation, Model Forecast Output)
        self.fc_sample_data_list = self.get_tar_data_dirs('fc_sample_data')   
    
    def get_data_dirs(self, data_type):
        """
        Extract list of all file directories in datasets' main directory (not derived from tar).
        
        Args:
            data_type (str): Foldername of dataset category of interest. 
                             Options:'input_model_data', 'fix_data', 'ne_data', 'fc_sample_data'
            
        Return (list): List of all file directories in datasets' main directory
        of interest.
        
        """
        # Dataset category.
        if data_type == 'input_model_data':
            suffix_fldr = 'input_model_data'
            avoid_fldrs = self.avoid_ma_fldrs
        elif data_type == 'fix_data':
            suffix_fldr = 'fix'
            avoid_fldrs = self.avoid_fix_fldrs
        elif data_type == 'ne_data':
            suffix_fldr = 'NaturalEarth'
            avoid_fldrs = self.avoid_ne_fldrs
        elif data_type == 'fc_sample_data':
            suffix_fldr = 'TBD???'
            avoid_fldrs = self.avoid_fc_sample_fldrs
        else:
            print(f"{data_type} does not exist")
          
        # Generate list of all file directories residing w/in datasets' 
        # main directory of interest. 
        file_dirs = []
        file_size = []
        root_dirs = []
        
        # ** TODO: Grab the root of the folders of interests and set as an argument to class for non-tar 
        # situations. If tar is being transferred, set to "./" + suffix_fldr**
        for root_dir, subfolders, filenames in os.walk("/home/schin/work/noaa/fv3-cam/UFS_SRW_App/develop/" + suffix_fldr, followlinks=True):
            root_dirs.append(root_dir)
            for file in filenames:
                file_dirs.append(os.path.join(root_dir, file))
        
        # List of all data folders/files in datasets' main directory of interest.
        
        # ** TODO: Grab the root of the folders of interests and set as an argument to class for non-tar 
        # situations. If tar is being transferred, set to "./" + suffix_fldr**
        root_list = os.listdir("/home/schin/work/noaa/fv3-cam/UFS_SRW_App/develop/" + suffix_fldr)
        print("\033[1m" +\
              "\nAll Primary Dataset Folders In SRW's " +\
              f"{data_type} Data Directory:" +\
              f"\n\n\033[0m{root_dirs}\n")
        
        # Removal of personal names.
        if avoid_fldrs != None:
            file_dirs = [x for x in file_dirs if any(x for name in avoid_fldrs if name not in x)]
        
        return file_dirs
    
    def get_tar_data_dirs(self, dataset_type):
        """
        Extract list of all file directories in datasets' main directory (tar).
        
        Args:
            tar_data_dir (str):
            dataset_type (str):
            
        Return (list): List of filtered file directories in datasets' extracted 
        from source directory.
        
        """
        # List of all directories from source.
        # Declare data directories of tar data files.
        if dataset_type == 'fix_data':
            tar_data_dir = self.fix_data_dir
            avoid = ['fix/fix_am/co2dat_4a', 
                     'fix/fix_orog',
                     'fix', 
                     'fix/fix_am', 
                     'fix/fix_am/fix_co2_proj', 
                     'fix/fix_aer', 
                     'fix/fix_sfc_climo', 
                     'fix/fix_lut']
            
        elif dataset_type == 'input_model_data':
            tar_data_dir = self.input_model_data_dir
            avoid = [ 'input_model_data/FV3GFS',
                      'input_model_data/RAP',
                      'input_model_data/HRRR',
                      'input_model_data/NAM',
                      'input_model_data/GSMGFS']

        elif dataset_type == 'ne_data':
            tar_data_dir = self.ne_data_dir
            avoid = []
        
        elif dataset_type == 'fc_sample_data': 
            tar_data_dir = self.fc_sample_data
            avoid = []
        
        # Open file in read mode.
        file_obj = tarfile.open(tar_data_dir,"r")

        # List of file directories in tar
        tar_file_list = []
        for f_dir in file_obj.getmembers():
            tar_file_list.append(f_dir.name)
        tar_file_list.sort()
        print(f"\nObtained list of files from {dataset_type} source.")
        print(f"Total Files: {len(tar_file_list)}")
        
        # Filtered directories from source.
        file_obj.extractall(members=[x for x in file_obj.getmembers() if x.name not in avoid])
        print(f"Filtered files from {dataset_type} source extracted to working dir.")
        
        return tar_file_list

    def get_model_analysis_data(self):
        """
        Extract list of all external model analysis file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the file directories into the
        external model for which generated the model analysis data file.

        """
        
        # Extract list of all external model analysis file directories.
        partition_ma_datasets = defaultdict(list) 
        for file_dir in self.ma_file_dirs:

            # FV3GFS data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['FV3GFS']):
                partition_ma_datasets['FV3GFS'].append(file_dir.replace("./", ""))

            # GSMGFS data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['GSMGFS']):
                partition_ma_datasets['GSMGFS'].append(file_dir.replace("./", ""))
                
            # HRRR data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['HRRR']):
                partition_ma_datasets['HRRR'].append(file_dir.replace("./", ""))
                
            # NAM data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['NAM']):
                partition_ma_datasets['NAM'].append(file_dir.replace("./", ""))
                
            # RAP data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['RAP']):
                partition_ma_datasets['RAP'].append(file_dir.replace("./", ""))

        return partition_ma_datasets    
    
    def get_specific_model_analysis_files(self, fv3gfs_ts, gsmgfs_ts, hrrr_ts, nam_ts, rap_ts):
        """
        Filters directory paths to timestamps of interest.
        
        Args: 
            fv3gfs_ts (list): List of FV3GFS timestamps to upload to cloud.
            gsmgfs_ts (list): List of GSMGFS timestamps to upload to cloud.
            hrrr_ts (list): List of HRRR input timestamps to upload to cloud.
            nam_ts (list): List of NAM timestamps to upload to cloud.
            rap_ts(list): List of RAP timestamps to upload to cloud.
                                  
        Return (dict): Dictionary partitioning the file directories into the
        timestamps of interest specified by user.
        
        """
        
        # Create dictionary mapping the user's request of timestamps.
        specific_ts_dict = defaultdict(list)
        specific_ts_dict['FV3GFS'] = fv3gfs_ts
        specific_ts_dict['GSMGFS'] = gsmgfs_ts
        specific_ts_dict['HRRR'] = hrrr_ts
        specific_ts_dict['NAM'] = nam_ts
        specific_ts_dict['RAP'] = rap_ts
        
        # Filter to directory paths of the timestamps specified by user.
        filter2specific_ts_datasets = defaultdict(list) 
        for dataset_type, timestamps in specific_ts_dict.items():
            
            # Extracts ext. model analysis files within the timestamps captured from user.
            if dataset_type == 'FV3GFS':
                for subfolder in self.partition_ma_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps) and any(folder_type not in subfolder for folder_type in ['input_model_data']):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'GSMGFS':
                for subfolder in self.partition_ma_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps) and any(folder_type not in subfolder for folder_type in ['input_model_data']):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'HRRR':
                for subfolder in self.partition_ma_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps) and any(folder_type not in subfolder for folder_type in ['input_model_data']):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)

            if dataset_type == 'NAM':
                for subfolder in self.partition_ma_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps) and any(folder_type not in subfolder for folder_type in ['input_model_data']):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)
                        
            if dataset_type == 'RAP':
                for subfolder in self.partition_ma_datasets[dataset_type]:
                    if any(ts in subfolder for ts in timestamps) and any(folder_type not in subfolder for folder_type in ['input_model_data']):
                        filter2specific_ts_datasets[dataset_type].append(subfolder)                        
 
        return filter2specific_ts_datasets    

    def get_fixed_data(self):
        """
        Extract list of all fixed file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_fix_datasets = defaultdict(list) 
        for file_dir in self.fix_file_dirs:

            # Fixed aer data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_aer']):
                partition_fix_datasets['fix_aer'].append(file_dir.replace("./", ""))

            # fixed am data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_am']):
                partition_fix_datasets['fix_am'].append(file_dir.replace("./", ""))
                
            # Fixed lut data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_lut']):
                partition_fix_datasets['fix_lut'].append(file_dir.replace("./", ""))
                
            # Fixed orog data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_orog']):
                partition_fix_datasets['fix_orog'].append(file_dir.replace("./", ""))
                
            # Fixed orog data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_sfc_climo']):
                partition_fix_datasets['fix_sfc_climo'].append(file_dir.replace("./", ""))

        return partition_fix_datasets    
    
#     def get_specific_grid_fixed_files(self, rrfs_conus_res, rrfs_conus_compact_res, rrfs_subconus_res, gfdl_res):
#         """
#         Filters directory paths to resolutions of interest.
        
#         Args: 
#             rrfs_conus_ts (list): List of RRFS_CONUS fixed data to upload to cloud.
#             rrfs_conus_compact_ts (list): List of compacted RRFS_CONUS fixed data to upload to cloud.
#             rrfs_subconus_ts (list): List of RRFS_SUBCONUS fixed data to upload to cloud.
#             gfdl_ts (list): List of GFDL fixed data to upload to cloud.
                                  
#         Return (dict): Dictionary partitioning the grid fixed data file directories into the
#         resolutions of interest specified by user.
        
#         **TODO: Keep if the data structure for SRW fixed files remains otherwise modify appropriately.
        
#         """
        
#         # Create dictionary mapping the user's request of resolutions.
#         specific_res_dict = defaultdict(list)
#         specific_res_dict['RRFS_CONUS'] = rrfs_conus_res
#         specific_res_dict['RRFS_CONUScompact'] =  rrfs_conus_compact_res
#         specific_res_dict['RRFS_SUBCONUS'] = rrfs_subconus_res
#         specific_res_dict['GFDLgrid'] = gfdl_res
        
#         # Filter to directory paths of the timestamps specified by user.
#         filter2specific_res_datasets = defaultdict(list) 
#         for grid_type, resolutions in specific_res_dict.items():
            
#             # Extracts ext. model analysis files within the timestamps captured from user.
#             if grid_type == 'RRFS_CONUS':
#                 for subfolder in self.partition_fixed_datasets[grid_type]:
#                     if any(res in subfolder for res in resolutions):
#                         filter2specific_res_datasets[grid_type].append(subfolder)

#             if grid_type == 'RRFS_CONUScompact':
#                 for subfolder in self.partition_fixed_datasets[grid_type]:
#                     if any(res in subfolder for res in resolutions):
#                         filter2specific_res_datasets[grid_type].append(subfolder)

#             if grid_type == 'RRFS_SUBCONUS':
#                 for subfolder in self.partition_fixed_datasets[grid_type]:
#                     if any(res in subfolder for res in resolutions):
#                         filter2specific_res_datasets[grid_type].append(subfolder)

#             if grid_type == 'GFDLgrid':
#                 for subfolder in self.partition_fixed_datasets[grid_type]:
#                     if any(res in subfolder for res in resolutions):
#                         filter2specific_res_datasets[grid_type].append(subfolder)
                        
#         return filter2specific_res_datasets    

    def get_ne_data(self):
        """
        Extract list of all Natural Eartch file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_ne_datasets = defaultdict(list) 
        for file_dir in self.ne_dirs:

            # Fixed raster data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['raster_files']):
                partition_ne_datasets['raster_files'].append(file_dir.replace("./", ""))

            # fixed shapefiles data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['shapefiles']):
                partition_ne_datasets['shapefiles'].append(file_dir.replace("./", ""))

        return partition_ne_datasets    

    def get_fc_data(self):
        """
        Extract list of all forecast sample file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_fc_datasets = defaultdict(list) 
        for file_dir in self.fc_sample_dirs:

            # Fixed raster data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['raster_files']):
                partition_fc_datasets['raster_files'].append(file_dir.replace("./", ""))

            # fixed shapefiles data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['shapefiles']):
                partition_fc_datasets['shapefiles'].append(file_dir.replace("./", ""))

        return partition_fc_datasets   