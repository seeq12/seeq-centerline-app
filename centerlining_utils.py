from imports import *
from config_settings import *
items_api = sdk.ItemsApi(spy.client)





def extract_grade(path):
    if path is None:
        return ''
    # Check if 'Grades' is in the path
    if 'Grades >>' in path:
        # Split the path at 'Grades >>' and take the element after it, which should be the grade
        return path.split('Grades >>')[-1].strip()
    # If 'Grades' is not in the path, or there is no grade after 'Grades >>', return ''
    return ''

def get_centerlining_sets(directory_path):
    '''
    Find items to fill the dropdown under Find Grade Find Centerlining Set drop down.
    '''

    # Get a list of all files and directories in the specified path
    all_items = os.listdir(directory_path)

    # Filter out directories to keep only files
    files = [item for item in all_items if os.path.isfile(os.path.join(directory_path, item))]
    
    if len(files) > 0:
        # Trees exist

        # Regular expression pattern to match text before '_centerline_tag_search' or '_centerline_metadata'
        pattern = r'^(.*?)_(?:centerline_tag_search|centerline_metadata)'

        # Extract unique text using list comprehension and regular expression
        unique_texts = set(re.match(pattern, filename).group(1) for filename in files if re.match(pattern, filename))

        # Convert the set to a list
        unique_texts_list = list(unique_texts)
        centerlining_tree_selections = unique_texts_list
    else:
        centerlining_tree_selections = ['No Centerlining Sets Exist Yet.']
    
    
    return centerlining_tree_selections

def delete_tree(tree_to_delete):
    # Get files
    # change metadata file Archived = True
    # push metadata
    # delete files
    files = get_tree_files(tree_to_delete)
    
    metadata_file = [file for file in files if 'centerline_metadata' in file][0]
    metadata_file_df = pd.read_csv(metadata_file, usecols=lambda column: column not in ['Unnamed: 0'])
    top_level_asset = spy.search({'Name': tree_to_delete, 'Type': 'Asset'}, workbook = centerlining_workbook, quiet = True)
    metadata_df = pd.concat([top_level_asset, metadata_file_df])
    
    try:
        for i, row in metadata_df.iterrows():
            id_value=row['ID']
            items_api.archive_item(id=id_value)
            # body_value={
            # "value": True
            # }
    except Exception as e:
        print(f"Error while deleting the tree: {e}")
        return
    
    
    delete_tree_files(files)
    
    return

def get_tree_files(tree_to_delete):
    # Construct the search pattern
    pattern = os.path.join(CENTERLINE_FOLDER, tree_to_delete + "*")

    # Find all files that match the pattern
    matching_files = glob.glob(pattern)

    # Manually filter for case sensitivity 
    case_sensitive_matching_files = [file for file in matching_files if os.path.basename(file).startswith(tree_to_delete)]
    
    return case_sensitive_matching_files

def delete_tree_files(files):
    # Iterate over the list of files and delete each one
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except OSError as e:
            print(f"Error: {e.strerror}, while trying to delete file: {file_path}")
    return



# Define conditional style function for Priority cell shading
def cell_style(params):
    if params['value'] == 1:
        return {'backgroundColor': 'yellow'}
    elif params['value'] == 2:
        return {'backgroundColor': 'red'}
    else:
        return {}


def convert_date_format(date_string):
    # parse date_string into a datetime object
    date_obj = datetime.strptime(date_string, "%Y-%m-%dT%H:%M%z")

    # convert datetime object into the desired format
    new_date_string = date_obj.strftime("%m/%d/%Y: %I:%M %p")
    
    return new_date_string


def find_previous_runs(grade, grade_condition_df):
    weeks = 1 
    now_dt = datetime.now(timezone('US/Central'))
    start_date_dt =  now_dt - timedelta(weeks = weeks)
    now_str = datetime.isoformat(now_dt, timespec = 'minutes') 
    start_date_str = datetime.isoformat(start_date_dt, timespec = 'minutes') 
    end_date_str = now_str
    runs = pd.DataFrame()

    calculation = f"$condition.keep('Grade Code', isEqualTo('{grade}')).removeShorterThan(30min).removeLongerThan(100d)"

    while len(runs) == 0:

        runs = spy.pull(items = grade_condition_df, 
                        calculation = calculation, 
                        start = start_date_str, 
                        end = end_date_str, 
                        quiet = True, 
                        shape = 'capsules',
                        capsule_properties = ['Duration', 'Grade Code']
                       )
        weeks = weeks + 1
        start_date_dt =  now_dt - timedelta(weeks = weeks)
        start_date_str = datetime.isoformat(start_date_dt, timespec = 'minutes') 
    return runs


def find_current_values(df):
    seconds = 1
    now_dt = datetime.now(timezone('US/Central'))
    start_date_dt =  now_dt - timedelta(seconds = seconds)
    now_str = datetime.isoformat(now_dt, timespec = 'minutes') 
    start_date_str = datetime.isoformat(start_date_dt, timespec = 'minutes') 
    end_date_str = now_str
    
    data = spy.pull(items = df, 
                    # header = 'Name',
                    calculation = f"$signal.resampleHold(1day, 1min)", 
                    start = start_date_str, 
                    end = now_str,
                    grid = f"{seconds}s", ### Check the seconds parameter
                    quiet = True
                   )
    
    # Step 1: Identifying duplicates in the last parts
    last_part_counts = {}
    for col in data.columns:
        if '>>' in col:
            last_part = col.split(' >> ')[-1]
            if last_part not in last_part_counts:
                last_part_counts[last_part] = 0
            last_part_counts[last_part] += 1

    # Step 2: Renaming columns based on duplicates
    new_column_names = {}
    for col in data.columns:
        if '>>' in col:
            parts = col.split(' >> ')
            last_part = parts[-1]

            # If the last part is a duplicate, join the second-to-last and last parts with ' / '
            if last_part_counts[last_part] > 1:
                new_name = ' / '.join(parts[-2:])
            else:
                new_name = last_part  # Use only the last part if it's not a duplicate

            new_column_names[col] = new_name
        else:
            new_column_names[col] = col  # Keep the original name if '>>' is not found

    # Renaming the DataFrame columns
    data.rename(columns=new_column_names, inplace=True)

    
    if data.shape[0] > 1:
        data = data.head(1)
    data = data.T
    data.index.name = 'Tags'
    data.rename(columns={data.columns[0]: 'Value'}, inplace=True)
    data.reset_index(inplace = True)
    data['Value'] = data['Value'].round(3)
        
    return data, now_str

def find_previous_run_data(grade, centerline_metadata):
    
    grade_condition_df = centerline_metadata[centerline_metadata['Name'] == 'Grade Condition'].iloc[0]
    previous_runs = find_previous_runs(grade, grade_condition_df)
    latest_run = previous_runs.tail(1)
    last_run_end = latest_run.iloc[0]['Capsule End']
    filtered_metadata = centerline_metadata[(centerline_metadata['Grade'] == grade)& centerline_metadata['Name'].isin(centerlining_metrics)]
    data = spy.pull(items = filtered_metadata, 
                    header = 'Name',
                    group_by = 'Asset',
                    calculation = f"$signal.resampleHold(1day, 1min)", 
                    start = last_run_end - pd.Timedelta(seconds=1), 
                    end = last_run_end,
                    grid = '1s',
                    quiet = True
                   )
    # Get the last outer index value
    last_outer_index = data.index.get_level_values(0)[-1]
    # Filter the DataFrame to retrieve all rows from the last outer index
    data = data.loc[last_outer_index]
    data.reset_index(inplace = True)
    data.rename(columns = {'Asset': 'Tags'},inplace=True)
    # Round only numeric columns
    numeric_cols = data.select_dtypes(include=np.number).columns
    data[numeric_cols] = data[numeric_cols].round(3)
    return data

def replace_non_json_compliant_floats(x):

    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):

        return None

    return x

def calculate_priority(row):
    cv, ll, l, h, hh = row['Current Value'], row['LL'], row['L'], row['H'], row['HH']
    if cv > hh:
        return (2 + ((cv - hh) / hh)) if hh != 0 else (cv - hh)
    elif cv < ll:
        return 2 + ((ll - cv) / ll) if ll != 0 else (cv - ll)
    elif cv > h:
        return 1 + ((cv - h) / h) if h != 0 else (cv - h)
    elif cv < l:
        return 1 + ((l - cv) / l) if l != 0 else (cv - l)
    else:
        return 0

# Find columns that end with specific text
def find_column(df, suffix):
    for col in df.columns:
        if col.endswith(suffix):
            return col
    return None

def add_priority(current, previous, grade):
    previous['Tags'] = previous['Tags'].str.strip()  # remove leading/trailing whitespace
    df = pd.merge(previous, current, on='Tags')
    
    upper_limit_inner_col = find_column(df, 'Upper Limit (Inner)')
    upper_limit_outer_col = find_column(df, 'Upper Limit (Outer)')
    lower_limit_inner_col = find_column(df, 'Lower Limit (Inner)')
    lower_limit_outer_col = find_column(df, 'Lower Limit (Outer)')

    # Defining conditions
    conditions_message = [
        ((df['Value'] > df[upper_limit_inner_col]) & (df['Value'] < df[upper_limit_outer_col])),
        ((df['Value'] < df[lower_limit_inner_col]) & (df['Value'] > df[lower_limit_outer_col])),
        (df['Value'] > df[upper_limit_outer_col]),
        (df['Value'] < df[lower_limit_outer_col])
    ]
    
    choices_message = ['High', 'Low', 'High High', 'Low Low']
    

    # Create new columns
    df['Status'] = np.select(conditions_message, choices_message, default='In Limits')

    # Renaming columns with dynamic names
    rename_dict = {
        'Value': 'Current Value',
        upper_limit_outer_col: 'HH',
        upper_limit_inner_col: 'H',
        lower_limit_outer_col: 'LL',
        lower_limit_inner_col: 'L',
         'Tags': 'Tag Name'}

    # Applying the renaming
    df.rename(columns=rename_dict, inplace=True)
    df['Priority'] = df.apply(calculate_priority, axis=1)
    df['Priority'] = df['Priority'].round(3)
    
    # Take the intersection of the desired order and the existing columns
    available_columns = list(set(column_order) & set(df.columns))

    # Preserve the order of columns in the original list
    available_columns = [col for col in column_order if col in available_columns]

    # Reorder the DataFrame
    df = df[available_columns]
    return df

def build_url(current, previous, grade, tag_name, tree, base_url, centerlining_workbook):
    
    worksheetName = f"Investigate Grade: {grade} Item: {tag_name}"
    
    if '/' in tag_name:
        tag_name = tag_name.replace('/', '%2F')
    
    if centerlining_workbook:
        workbookName = centerlining_workbook
    else:
        workbookName = "Centerline_Investigation"
        
    lower_limit_inner_path = f"{tree} >> Grades >> {grade} >> {tag_name} >> Lower Limit (Inner)"
    lower_limit_outer_path = f"{tree} >> Grades >> {grade} >> {tag_name} >> Lower Limit (Outer)"
    upper_limit_inner_path = f"{tree} >> Grades >> {grade} >> {tag_name} >> Upper Limit (Inner)"
    upper_limit_outer_path = f"{tree} >> Grades >> {grade} >> {tag_name} >> Upper Limit (Outer)"
    average_path = f"{tree} >> Grades >> {grade} >> {tag_name} >> Average (Target)"
    raw_tag = f"{tree} >> Grades >> {grade} >> {tag_name} >> Raw Tag"
    display_start = "1d"
    url = f"{base_url}workbook/builder?workbookName={workbookName}&worksheetName={worksheetName}&trendItems={lower_limit_inner_path}&trendItems={lower_limit_outer_path}&trendItems={upper_limit_inner_path}&trendItems={upper_limit_outer_path}&trendItems={average_path}&trendItems={raw_tag}&displayStartTime=*-{display_start}&displayEndTime=*"
    
    return url












        
def create_tree(name_of_tree, tags_df, speed_tag_df, input_config_values, unique_grades_list, grade_tag_df, downtime_tag_df, string_or_number, uptime_value, centerlining_workbook, centerlining_datasource, CENTERLINE_FOLDER_PATH):
    topLevelAsset = name_of_tree
    list_of_grades = unique_grades_list
    raw_upload = tags_df.copy()
    if any(isinstance(item_type, str) for item_type in list_of_grades):
        string_grades = True
    else:
        string_grades = False
        list_of_grades = [str(int(round(item))) for item in list_of_grades]
    
    tags_df.rename(columns = {'Tag Name': 'Name'}, inplace = True)
    tags_df['Type'] = 'Signal'
    
    unsearchable_columns = ['Friendly Name', 'Category']
    search_columns = [col for col in tags_df.columns if col not in unsearchable_columns]
    
    # Make the search case insensitive
    search_df = tags_df.copy()
    search_df['Name'] = '/^' + search_df['Name'] + '$/i'
    display("Finding all tags.")
    tag_search = spy.search(search_df[search_columns], quiet = True, limit = None)
    # Check for duplicate names in tags_df
    if tags_df['Name'].duplicated().any():
        # If duplicates exist, merge on both 'Name' and 'Asset'
        # Create temporary lowercase columns for 'Name' and 'Asset' in both dataframes
        tags_df['Name_lower'] = tags_df['Name'].str.lower()
        tags_df['Asset_lower'] = tags_df['Asset'].str.lower()
        tag_search['Name_lower'] = tag_search['Name'].str.lower()
        tag_search['Asset_lower'] = tag_search['Asset'].str.lower()

        # Perform the merge using the temporary lowercase columns
        metadata_df = tags_df.merge(tag_search[['ID', 'Description', 'Asset', 'Name', 'Name_lower', 'Asset_lower']], 
                                  how='left', 
                                  left_on=['Name_lower', 'Asset_lower'], 
                                  right_on=['Name_lower', 'Asset_lower'])

        # Rename columns to keep the correct casing
        metadata_df.rename(columns={'Name_y': 'Name', 'Asset_y': 'Asset'}, inplace=True)

        # Drop redundant columns
        metadata_df.drop(['Name_x', 'Asset_x', 'Name_lower', 'Asset_lower'], axis=1, inplace=True)
        # Update 'Name' only where 'Asset' is not empty or NaN
        metadata_df.loc[metadata_df['Asset'].notna() & (metadata_df['Asset'] != ''), 'Name'] = metadata_df['Asset'] + ' / ' + metadata_df['Name']

        
    else:
        # If no duplicates, merge on 'Name' only
        tags_df['Name_lower'] = tags_df['Name'].str.lower()
        tag_search['Name_lower'] = tag_search['Name'].str.lower()

        # Perform the merge using the temporary lowercase columns
        metadata_df = tags_df.merge(tag_search, 
                                  how='left', 
                                  left_on=['Name_lower'], 
                                  right_on=['Name_lower'])

        # Rename columns to keep the correct casing
        metadata_df.rename(columns={'Name_y': 'Name'}, inplace=True)

        # Drop redundant columns
        metadata_df.drop(['Name_x', 'Name_lower'], axis=1, inplace=True)
        

    metadata_df = metadata_df.copy().loc[~metadata_df['ID'].isna()]
    metadata_df['Tag Name'] = metadata_df['Name']
    metadata_df.fillna('', inplace=True)
    metadata_df['Build Asset'] = topLevelAsset 
    metadata_df['Build Path'] = None
    
    count = 1
    display("Creating Grade Variants for all tags")
    for grade in list_of_grades:
        grade_info = metadata_df.copy()
        grade_info['Grade'] = grade
        if count == 1:
            all_grades_df = grade_info
        else:
            all_grades_df = pd.concat([all_grades_df, grade_info], ignore_index=True)
        count = count + 1
    
    all_grades_df['Grades Category'] = 'Grades'
    all_grades_df['Grade Configuration'] = 'Grade Configuration'
    
    
    # All Config Attributes (input_config_values)
    # {'Upper Speed Limit Filter': ,
    #  'Lower Speed Limit Filter': , 
    #  '% Uptime Filter': , 
    #  'StdDev Multiplier (Inner)': , 
    #  'StdDev Multiplier (Outer)': , 
    #  'Remove Shorter Than (Shortest Grade Run)': , 
    #  'Remove Longer Than (Longest Grade Run)': , 
    #  'Number of Previous Grade Runs for Limit Creation': , 
    #  'Lookback Range for Previous Grade Runs': , 
    #  'Sampling Rate': }
    
    #### Centerlining Asset Tree
    class CenterliningTree(Asset):

        @Asset.Component() 
        def Config(self, metadata):

            return self.build_components(template = Grades_Config, metadata = metadata, column_name = 'Grade Configuration')

        @Asset.Component() 
        def Grades_category(self, metadata):

            return self.build_components(template = Grades, metadata = metadata, column_name = 'Grades Category')    

    class Grades_Config(Asset):
        @Asset.Component() 
        def Grades_config(self, metadata):

            return self.build_components(template = Config_Parameters, metadata = metadata, column_name = 'Grade')


    class Grades(Asset):


        @Asset.Component() 
        def Grades_breakdown(self, metadata):

            return self.build_components(template = All_Tags, metadata = metadata, column_name = 'Grade')






    class Config_Parameters(Asset):
        @Asset.Attribute()
        def upper_speed_filter(self, metadata):
            name = 'Upper Speed Limit Filter'
            description = 'Only data within the Upper and Lower Speed Limit Filter limits will be considered for limit creation.'
            speed_filter_signal = speed_tag_df['Name'].iloc[0]
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Only data within the Upper and Lower Speed Limit Filter limits will be considered for limit creation.\n//The tag being referenced for speed is: {speed_filter_signal}\n{default}",
                    'Description': description
                }

        @Asset.Attribute()
        def lower_speed_filter(self, metadata):
            name = 'Lower Speed Limit Filter'
            description = 'Only data within the Upper and Lower Speed Limit Filter limits will be considered for limit creation.'
            speed_filter_signal = speed_tag_df['Name'].iloc[0]
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Only data within the Upper and Lower Speed Limit Filter limits will be considered for limit creation.\n//The tag being referenced for speed is: {speed_filter_signal}\n{default}",
                    'Description': description
                }


        @Asset.Attribute()
        def uptime_filter(self, metadata):
            name = '% Uptime Filter'
            description = 'Only grade runs with % Uptime greater than this threshold will be considered for limit creation'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Only grade runs with % Uptime greater than the below value will be considered for limit creation.\n{default}",
                    'Description': description
                }

        @Asset.Attribute()
        def remove_shorter_than(self, metadata):
            name = 'Remove Shorter Than (Shortest Grade Run)'
            description = 'Shortest expected grade run'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula': f"// Edit the below to indicate the shortest expected grade run.\n{default}",
                    'Description': description
                }

        @Asset.Attribute()
        def remove_longer_than(self, metadata):
            name = 'Remove Longer Than (Longest Grade Run)'
            description = 'Longest expected grade run'
            default = input_config_values[name]
            return {
                    'Name': 'Remove Longer Than (Longest Grade Run)',
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate the shortest expected grade run.\n{default}",
                    'Description': description
                }

        @Asset.Attribute()
        def Number_of_Previous_Grade_Runs(self, metadata):
            name = 'Number of Previous Grade Runs for Limit Creation'
            description = 'Number of previous grade runs to consider'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate how many previous grade runs to consider in the calculation of limits.\n{default}",
                    'Description': description
                }


        @Asset.Attribute()
        def Lookback_of_Previous_Grade_Runs(self, metadata):
            name = 'Lookback Range for Previous Grade Runs'
            description = 'How far to look back for previous grade runs'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate how far to look back in time to find the number of grades indicated within Number of Previous Grade Runs for Limit Creation.\n{default}",
                    'Description': description
                }


        @Asset.Attribute()
        def StdDev_Multiplier_Inner(self, metadata):  
            name = 'StdDev Multiplier (Inner)'
            description = 'StdDev Inner Multiplier'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate how to calculate the inner limits... I.e. this scalar multiplied to the standard deviation.\n{default}",
                    'Description': description
                }

        @Asset.Attribute()
        def StdDev_Multiplier_Outer(self, metadata):   
            name = 'StdDev Multiplier (Outer)'
            description = 'StdDev Outer Multiplier'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate how to calculate the outer limits... I.e. this scalar multiplied to the standard deviation.\n{default}",
                    'Description': description
                }


        @Asset.Attribute()
        def sampling_rate(self, metadata):   
            name = 'Sampling Rate'
            description = 'Sampling rate to calculate limits'
            default = input_config_values[name]
            return {
                    'Name': name,
                    'Type':'Scalar',
                    'Formula':f"// Edit the below to indicate the sampling rate used to calculate limits.\n{default}",
                    'Description': description
                }


    class All_Tags(Asset):


        @Asset.Component() 
        def all_tags(self, metadata):

            return self.build_components(template = Item_Parameters, metadata = metadata, column_name = 'Name')

    #          Grade Condition    
        @Asset.Attribute()
        def GradeCondition(self,metadata):
            if string_grades:
                # no need to convert
                convert_to_string =''
            else:
                # convert to strings
                convert_to_string = '.toString()'
            
            return {
                'Name': 'Grade Condition',
                'Type':'Condition',
                'Formula':f'$signal{convert_to_string}.toCondition("Grade Code")',
                'Formula Parameters': {
                    '$signal': grade_tag_df['ID'].iloc[0]
                }
            }        
        
        

        @Asset.Attribute()
        def UptimeCondition(self,metadata):
            if string_or_number == 'String':
                formula = f'$signal == "{uptime_value}"'
            else:
                formula = f'$signal == {uptime_value}'
            
            return {
                'Name': 'Process Online',
                'Type':'Condition',
                'Formula': formula,
                'Formula Parameters': {
                    '$signal': downtime_tag_df['ID'].iloc[0]
                }
            }  

    #          Grade Keep Condition    
        @Asset.Attribute()
        def GradeKeepCondition(self,metadata):
            removeShorterThan = [
                asset.remove_shorter_than() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]

            removeLongerThan = [
                asset.remove_longer_than() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]
            

            return_item = {
                    'Name': 'Grade Keep Condition',
                    'Type':'Condition',
                    'Formula':f"$condition.keep('Grade Code', isEqualTo('{self.definition['Name']}'))\n.removeShorterThan($rst)\n.removeLongerThan($rlt)",
                    'Formula Parameters': {
                        '$condition': self.GradeCondition(),
                        '$rst': removeShorterThan[0],
                        '$rlt': removeLongerThan[0]
                    },
                    'Description': 'Grade Condition filtered for specific grade'
                }


            return return_item


    #          On Grade with No Sheet Break (UnFiltered)    
        @Asset.Attribute()
        def OnGrade_Running_UnFiltered(self,metadata): #OnGrade_NoBreak_UnFiltered

            return {
                'Name': 'On Grade While Running (UnFiltered)',
                'Type':'Condition',
                'Formula': '($on_grade and $online)',
                'Formula Parameters': {
                    '$on_grade': self.GradeKeepCondition(),
                    '$online': self.UptimeCondition()
                },
                'Description': 'On Grade AND Online Condition'
            }



    #          On Grade with No Sheet Break (Filtered)    
        @Asset.Attribute()
        def OnGrade_Running_Filtered(self,metadata): #OnGrade_NoBreak_Filtered
            removeShorterThan = [
                asset.remove_shorter_than() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]   
            return {
                'Name': 'On Grade While Running (Filtered by Duration)',
                'Type':'Condition',
                'Formula': '($unfiltered)\n.removeShorterThan($rst)',
                'Formula Parameters': {
                    '$unfiltered': self.OnGrade_Running_UnFiltered(),
                    '$rst': removeShorterThan[0]
                },
                'Description': 'On Grade AND Online Condition - Remove shorter than X'
            }    


    #          On Grade with No Sheet Break (Filtered by Uptime)    
        @Asset.Attribute()
        def OnGrade_Running_Filtered_Uptime_Speed(self,metadata): #OnGrade_NoBreak_Filtered_Uptime_Speed

            return {
                'Name': 'On Grade While Running (Filtered by Uptime and Speed)',
                'Type':'Condition',
                'Formula': '$grade and $online and $withinSpeed',
                'Formula Parameters': {
                    '$grade': self.Grade_Keep_Properties_Filtered(),
                    '$online': self.UptimeCondition(),
                    '$withinSpeed': self.Within_Speed_Thresholds()
                },
                'Description': 'On Grade where Uptime > Threshold AND Online'
            }      

    #          Within Speed Thresholds    
        @Asset.Attribute()
        def Within_Speed_Thresholds(self,metadata):        
            uppper_filter = [
                asset.upper_speed_filter() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]
            lower_filter = [
                asset.lower_speed_filter() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]

            return {
                'Name': 'Within Speed Thresholds',
                'Type':'Condition',
                'Formula': "$speed <= $upper and $speed >= $lower",
                'Formula Parameters': {
                    '$speed': speed_tag_df['ID'].iloc[0],
                    '$upper': uppper_filter[0],
                    '$lower': lower_filter[0]
                },
                'Description': 'Condition to identify when within defined speed thresholds'
            }   


    #          Grade Keep Condition with Properties    
        @Asset.Attribute()
        def Grade_Keep_Properties(self,metadata):        

            return {
                'Name': 'Grade Keep Condition with Uptime Property',
                'Type':'Condition',
                'Formula': "$gkc\n.setProperty('% Uptime',$gradeUnfiltered,percentDuration())\n.transform($capsules ->\n$capsules.setProperty('% Uptime',$capsules.property('% Uptime').round(2)))",
                'Formula Parameters': {
                    '$gkc': self.GradeKeepCondition(),
                    '$gradeUnfiltered': self.OnGrade_Running_UnFiltered()
                },
                'Description': 'Same as Grade Keep Condition but with % Uptime property'
            }    


    #          Grade Keep Condition with Properties FILTERED FOR UPTIME 
        @Asset.Attribute()
        def Grade_Keep_Properties_Filtered(self,metadata):        
            uptime_filter = [
                asset.uptime_filter() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]
            return {
                'Name': 'Grade Keep Condition Filtered by Uptime',
                'Type':'Condition',
                'Formula': "$gkcwUP.keep('% Uptime', isgreaterthan($filter))",
                'Formula Parameters': {
                    '$gkcwUP': self.Grade_Keep_Properties(),
                    '$filter': uptime_filter[0]
                },
                'Description': 'Grade Runs filtered by Uptime > specified value'
            } 

    #          Condition for Last N Grade Runs
        @Asset.Attribute()
        def last_n_grade_runs(self, metadata):

            nopg = [
                asset.Number_of_Previous_Grade_Runs() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]

            lrfp = [
                asset.Lookback_of_Previous_Grade_Runs() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.definition['Name']))
            ]


            return {
                'Name': 'Last N Grade Runs',
                'Type':'Condition',
                'Formula':'$condition.toCapsulesByCount($nopg, $lrfp, 1)',
                'Formula Parameters': {
                    '$condition': self.Grade_Keep_Properties_Filtered(),
                    '$nopg': nopg[0],
                    '$lrfp': lrfp[0]
                },
                'Description': 'Capsule for N consecutive grade runs within the specified range'
            }

    class Item_Parameters(Asset):

    #          Raw Tag    
        @Asset.Attribute()
        def Tag(self,metadata):
            return {
                'Name': 'Raw Tag',
                'Type':'Signal',
                'Formula':'$signal',
                'Formula Parameters': {
                    '$signal': metadata['ID'].iloc[0]
                },
                'Description': metadata['Description'].iloc[0]
            }

    #          Raw Tag Resampled   
        @Asset.Attribute()
        def Tag_resampled(self,metadata):
            period = [
                asset.sampling_rate() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            ]


            return {
                'Name': 'Raw Tag Resampled',
                'Type':'Signal',
                'Formula':'$signal.resample($period)',
                'Formula Parameters': {
                    '$signal': metadata['ID'].iloc[0],
                    '$period': period[0]
                },
                'Description': metadata['Description'].iloc[0]
            }



    #          Filtered Tag    
        @Asset.Attribute()
        def FilteredTag(self,metadata):
            return {
                'Name': 'Raw Tag within Grade, Speed, & While Running',
                'Type':'Signal',
                'Formula':'$signal.within($condition).setUncertainty(5min)',
                'Formula Parameters': {
                    '$signal': self.Tag_resampled(),
                    '$condition': self.parent.OnGrade_Running_Filtered_Uptime_Speed()
                },
                'Description': metadata['Description'].iloc[0]
            }


    #          Grade Keep Condition    
        @Asset.Attribute()
        def GradeKeepCondition2(self,metadata):        

            return {
                'Name': 'Grade Keep Condition',
                'Type':'Condition',
                'Formula':f"$condition",
                'Formula Parameters': {
                    '$condition': self.parent.GradeKeepCondition()
                },
                'Description': 'Grade Condition filtered for specific grade'
            } 


    #          Grade Keep Condition w/ property  
        @Asset.Attribute()
        def GradeKeepCondition_Properties2(self,metadata):        

            return {
                'Name': 'Grade Keep Condition with Uptime Property',
                'Type':'Condition',
                'Formula':f"$condition",
                'Formula Parameters': {
                    '$condition': self.parent.Grade_Keep_Properties()
                },
                'Description': 'Grade Condition filtered for specific grade with property of uptime'
            } 

    #          Grade Keep Condition Filtered 
        @Asset.Attribute()
        def GradeKeepCondition_Properties3(self,metadata):        

            return {
                'Name': 'Grade Keep Condition Filtered by Uptime',
                'Type':'Condition',
                'Formula':f"$condition",
                'Formula Parameters': {
                    '$condition': self.parent.Grade_Keep_Properties_Filtered()
                },
                'Description': 'Grade Condition filtered by Uptime'
            } 



    #          Raw Description    
        @Asset.Attribute()
        def Desc(self,metadata):
            return {
                'Name': 'Description',
                'Type':'Signal',
                'Formula':f"'{metadata['Description'].iloc[0]}'.toSignal()",
                'Description': metadata['Description'].iloc[0]
            }

    #          Friendly Name    
        @Asset.Attribute()
        def FriendlyName(self,metadata):
            return {
                'Name': 'Friendly Name',
                'Type':'Signal',
                'Formula':f"'{metadata['Friendly Name'].iloc[0]}'.toSignal()",
                'Description': metadata['Friendly Name'].iloc[0]
            }

    #          Category    
        @Asset.Attribute()
        def Category(self,metadata):
            return {
                'Name': 'Category',
                'Type':'Signal',
                'Formula':f"'{metadata['Category'].iloc[0]}'.toSignal()",
                'Description': metadata['Category'].iloc[0]
            }    





    ###### Statistics Calculations            

    #          Standard Deviation per Previous N Grade Runs    
        @Asset.Attribute()
        def stddev_last_n(self,metadata):

            # maxinterp = [
            #     asset.max_Interp() for asset in self.all_assets() 
            #     if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            # ]

            return {
                'Name': 'Standard Deviation per Previous N Grade Runs',
                'Type':'Signal',
                #'Formula':'$gradeStdDev = $signal.aggregate(stdDev(), $condition, endKey(), $maxInterp).toStep().resampleHold(3mo, 5min).setUncertainty(5min)\n$gradeStdDev',
                'Formula':'$gradeStdDev = $signal.aggregate(stdDev(), $condition, endKey()).toStep().resampleHold(3mo, 5min).setUncertainty(5min)\n$gradeStdDev',
                'Formula Parameters': {
                    '$signal': self.FilteredTag(),
                    '$condition': self.parent.last_n_grade_runs(),
                    #'$maxInterp': maxinterp[0]
                },
                'Description': 'Standard deviation across each grade run'
            }

    #          Average per Previous N Grade Runs (I.e. Target)    
        @Asset.Attribute()
        def avg_last_n(self,metadata):

            # maxinterp = [
            #     asset.max_Interp() for asset in self.all_assets() 
            #     if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            # ]

            return {
                'Name': 'Average (Target)',
                'Type':'Signal',
                #'Formula':'$gradeAvg = $signal.aggregate(average(), $condition, endKey(), $maxInterp).toStep().resampleHold(3mo, 5min).setUncertainty(5min)\n$gradeAvg',
                'Formula':'$gradeAvg = $signal.aggregate(average(), $condition, endKey()).toStep().resampleHold(3mo, 5min).setUncertainty(5min)\n$gradeAvg',
                'Formula Parameters': {
                    '$signal': self.FilteredTag(),
                    '$condition': self.parent.last_n_grade_runs(),
                    #'$maxInterp': maxinterp[0]
                },
                'Description': 'Average across each grade run'
            }



    #          Lower Limit (Inner)   
        @Asset.Attribute()
        def Lower_Inner(self,metadata):
            smi = [
                asset.StdDev_Multiplier_Inner() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            ]

            return {
                'Name': 'Lower Limit (Inner)',
                'Type':'Signal',
                'Formula':'$Lower =  $Average - $smi*$stdDev\n$Lower',
                'Formula Parameters': {
                    '$Average': self.avg_last_n(),
                    '$stdDev': self.stddev_last_n(),
                    '$smi': smi[0]
                },
                'Description': 'Inner Lower Limit considering N previous grade runs'
            }


    #          Lower Limit (Outer)   
        @Asset.Attribute()
        def Lower_Outer(self,metadata):
            smo = [
                asset.StdDev_Multiplier_Outer() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            ]

            return {
                'Name': 'Lower Limit (Outer)',
                'Type':'Signal',
                'Formula':'$Lower =  $Average - $smo*$stdDev\n$Lower',
                'Formula Parameters': {
                    '$Average': self.avg_last_n(),
                    '$stdDev': self.stddev_last_n(),
                    '$smo': smo[0]
                },
                'Description': 'Outer Lower Limit considering N previous grade runs'
            }



    #          Upper Limit (Inner)   
        @Asset.Attribute()
        def Upper_Inner(self,metadata):
            smi = [
                asset.StdDev_Multiplier_Inner() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            ]

            return {
                'Name': 'Upper Limit (Inner)',
                'Type':'Signal',
                'Formula':'$Lower =  $Average + $smi*$stdDev\n$Lower',
                'Formula Parameters': {
                    '$Average': self.avg_last_n(),
                    '$stdDev': self.stddev_last_n(),
                    '$smi': smi[0]
                },
                'Description': 'Inner Upper Limit considering N previous grade runs'
            }


    #          Upper Limit (Outer)   
        @Asset.Attribute()
        def Upper_Outer(self,metadata):
            smo = [
                asset.StdDev_Multiplier_Outer() for asset in self.all_assets() 
                if (isinstance(asset, Config_Parameters) and (asset.definition['Name'] == self.parent.definition['Name']))
            ]

            return {
                'Name': 'Upper Limit (Outer)',
                'Type':'Signal',
                'Formula':'$Lower =  $Average + $smo*$stdDev\n$Lower',
                'Formula Parameters': {
                    '$Average': self.avg_last_n(),
                    '$stdDev': self.stddev_last_n(),
                    '$smo': smo[0]
                },
                'Description': 'Outer Upper Limit considering N previous grade runs'
            }

    #          Outside of L or H
        @Asset.Attribute()
        def L_or_H(self, metadata):
            return {
                'Name': 'Outside of L or H',
                'Type':'Condition',
                'Formula':'($rt > $h or $rt < $l) and not (($rt < $ll or $rt > $hh))',
                'Formula Parameters': {
                    '$rt': self.Tag(),
                    '$h': self.Upper_Inner(),
                    '$l': self.Lower_Inner(),
                    '$ll': self.Lower_Outer(),
                    '$hh': self.Upper_Outer()
                },
                'Description': 'Raw Tag outside of inner boundaries, but not outer'
            }

        #          Outside of LL or HH
        @Asset.Attribute()
        def LL_or_HH(self, metadata):
            return {
                'Name': 'Outside of LL or HH',
                'Type':'Condition',
                'Formula':'($rt > $hh or $rt < $ll)',
                'Formula Parameters': {
                    '$rt': self.Tag(),
                    '$ll': self.Lower_Outer(),
                    '$hh': self.Upper_Outer()
                },
                'Description': 'Raw Tag outside of outer boundaries'
            }

        #          Raw Tag Invalid
        @Asset.Attribute()
        def Raw_Invalid(self, metadata):
            return {
                'Name': 'Raw Tag Not Valid',
                'Type':'Condition',
                'Formula':'$rt.isNotValid()',
                'Formula Parameters': {
                    '$rt': self.Tag()
                },
                'Description': 'Raw Tag value is invalid'
            }

        #          Priority
        @Asset.Attribute()
        def Priority(self, metadata):
            return {
                'Name': 'Priority',
                'Type':'Signal',
                'Formula':'0.tosignal()\n.splice(1.tosignal(), $oolo)\n.splice(2.tosignal(), $oolo2)\n.splice(SCALAR.INVALID.tosignal(), $rtnv)',
                'Formula Parameters': {
                    '$oolo': self.L_or_H(),
                    '$oolo2': self.LL_or_HH(),
                    '$rtnv': self.Raw_Invalid()
                },
                'Description': 'Priority of deviation. 1 if outside of inner limits. 2 if outside of outer limits.'
            }
    display("Building the Asset Tree... This may take a while.")
    build_df = spy.assets.build(CenterliningTree,all_grades_df, quiet = True)
    
    display("Pushing the Asset Tree... This may take a while.")
    asset_tree_push = spy.push(metadata = build_df, workbook = centerlining_workbook, datasource = centerlining_datasource, archive = True, quiet = True)
    tree_search = spy.search({'Path': topLevelAsset, 'Datasource ID':centerlining_datasource}, quiet = True, limit = None, workbook=centerlining_workbook)
    tree_search['Grade'] = tree_search['Path'].apply(extract_grade)
    
    display("Building CSV Files.")
    raw_upload.to_csv(f"{CENTERLINE_FOLDER_PATH}/{topLevelAsset}_raw_upload.csv")
    tree_search.to_csv(f"{CENTERLINE_FOLDER_PATH}/{topLevelAsset}_centerline_metadata.csv")
    tag_search.to_csv(f"{CENTERLINE_FOLDER_PATH}/{topLevelAsset}_centerline_tag_search.csv")
                   
    display("Complete.")
    