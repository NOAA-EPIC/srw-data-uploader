# Reading specific cases per csv file (https://docs.google.com/spreadsheets/d/18CO_OtLsLeRBMcW0O8YdS4ipancKva0Qospz1xaWIdY/edit#gid=0)
import pandas as pd
import numpy as np

class TransferCaseData():
    """
    Obtain directories for the datasets requested by the User from csv file in https://docs.google.com/spreadsheets/d/18CO_OtLsLeRBMcW0O8YdS4ipancKva0Qospz1xaWIdY/edit#gid=0
    
    """
    def __init__(self, srw_cases_fn = 'WE2E Cases and Locations.xlsx'):
        """
        Args: 
             linked_home_dir (str): User directory linked to the RDHPCS' root
                                    data directory.
             platform (str): RDHPCS of where the datasets will be sourced.
        """
    
        # Establish locality of where the csv file listing specific cases.
        
        # Read WE2E Cases.
        self.srw_cases_fn = srw_cases_fn
        self.we2e_cases_df = pd.read_excel(self.srw_cases_fn,
                                      sheet_name='E2E Cases')
    def read_srw_grids(self):
        """
        Read unique FV3LAM pregen grids specified by user.
        
        Args:
            None
            
        Return (list): List of FV3LAM pregen grids of interest.
        
        """        
        return [grid_case for grid_case in self.we2e_cases_df['Grid'].unique() if str(grid_case)!= 'nan']
    
    def read_srw_cases(self, delimiter_sym = ','):
        """
        Read unique model & datetime names specified by user.
        
        Args:
            delimiter_sym (str): Delimiter of the forecast hours.
        
        Return (list, list, list, list, list): List of the date & times of interest for 
        the external model analysis files from fv3gfs, gsmgfs, hrrr, rap, and nam_ts.
        
        """
        # Reformat Dates of WE2E Cases.
        self.we2e_cases_df['Date'] = self.we2e_cases_df['Date'].apply(lambda x: str(x.strftime('%Y%m%d')) if pd.notnull(x) else x)
        self.we2e_cases_df = pd.concat([self.we2e_cases_df, self.we2e_cases_df['Time (UTC)'].str.split(delimiter_sym, expand=True)], axis=1)

        # Consider Dates w/ Times of WE2E Cases.        
        for f in range(len(self.we2e_cases_df['Time (UTC)'].str.split(delimiter_sym, expand=True).columns)):
            self.we2e_cases_df[f] = self.we2e_cases_df[f].astype(str)
            self.we2e_cases_df.insert(7+f, f'Date_{f}', None)
            for idx, hr in self.we2e_cases_df[f].items():
                if hr!='None' and type(self.we2e_cases_df['Date'][idx])!=type(pd.NaT):
                    self.we2e_cases_df[f'Date_{f}'][idx] = str(self.we2e_cases_df['Date'][idx]) + str(hr)

        # Unique LBCs and ICs to dataset dates merge into a single columns          
        all_cases = self.we2e_cases_df.groupby(['ICS', 'LBCS', f'Date_0']).size().reset_index(name='Freq')  
        for f in range(1, len(self.we2e_cases_df['Time (UTC)'].str.split(delimiter_sym, expand=True).columns)):
            iter_case = self.we2e_cases_df.groupby(['ICS', 'LBCS', f'Date_{f}']).size().reset_index(name='Freq') 
            all_cases = all_cases.append(iter_case)
        all_cases.reset_index()            
        all_cases['Date'] = all_cases['Date_0']
        for f in range(1, len(self.we2e_cases_df['Time (UTC)'].str.split(delimiter_sym, expand=True).columns)):       
            all_cases['Date'] = all_cases['Date'].combine_first(all_cases[f'Date_{f}'])

        # Account for all external model's ICs dates.
        ics_fv3gfs = all_cases[all_cases["ICS"]=='FV3GFS']
        ics_gsmgfs = all_cases[all_cases["ICS"]=='GSMGFS']
        ics_hrrr = all_cases[all_cases["ICS"]=='HRRR']
        ics_rap = all_cases[all_cases["ICS"]=='RAP']
        ics_nam = all_cases[all_cases["ICS"]=='NAM']

        # Account for all external model's LBCs dates.
        lbcs_fv3gfs = all_cases[all_cases["LBCS"]=='FV3GFS']
        lbcs_gsmgfs = all_cases[all_cases["LBCS"]=='GSMGFS']
        lbcs_hrrr = all_cases[all_cases["LBCS"]=='HRRR']
        lbcs_rap = all_cases[all_cases["LBCS"]=='RAP']
        lbcs_nam = all_cases[all_cases["LBCS"]=='NAM']

        # Account for all external model's dates.
        fv3gfs_ts = list(np.unique(list(ics_fv3gfs['Date']) + list(lbcs_fv3gfs['Date'])))
        gsmgfs_ts = list(np.unique(list(ics_gsmgfs['Date']) + list(lbcs_gsmgfs['Date'])))
        hrrr_ts = list(np.unique(list(ics_hrrr['Date']) + list(lbcs_hrrr['Date'])))
        rap_ts = list(np.unique(list(ics_rap['Date']) + list(lbcs_rap['Date'])))
        nam_ts = list(np.unique(list(ics_nam['Date']) + list(lbcs_nam['Date'])))

        print(f"\nDataset dates for FV3GFS:\n{fv3gfs_ts}")
        print(f"\nDataset dates for GSMGFS:\n{gsmgfs_ts}")
        print(f"\nDataset dates for HRRR:\n{hrrr_ts}")
        print(f"\nDataset dates for RAP:\n{rap_ts}")
        print(f"\nDataset dates for NAM:\n{nam_ts}")
        
        return fv3gfs_ts, gsmgfs_ts, hrrr_ts, rap_ts, nam_ts

        
if __name__ == '__main__': 
    
    # Obtain directories for the datasets requested by the User from csv file in https://docs.google.com/spreadsheets/d/18CO_OtLsLeRBMcW0O8YdS4ipancKva0Qospz1xaWIdY/edit#gid=0
    fv3gfs_ts, gsmgfs_ts, hrrr_ts, rap_ts, nam_ts = TransferCaseData(srw_cases_fn = 'WE2E Cases and Locations.xlsx').read_srw_cases()
    
    
    

