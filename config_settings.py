base_url = "https://develop.seeq.dev/"
CENTERLINE_FOLDER = "Centerlining Datasets"
centerlining_workbook = '0EEA5D02-D5C7-EA40-842F-D05BBA007386'
centerlining_datasource = 'Seeq Centerlining'

log_file = 'centerline_app.log'

update_frequency = 60 # seconds - Main Centerline Table
update = True

GREEN = '#427861'
RED = '#FF0000'
app_color = '#003057' # Dark Blue

centerlining_metrics = ['Average (Target)',
                       'Category',
                       'Description',
                       'Friendly Name',
                       'Lower Limit (Inner)',
                       'Lower Limit (Outer)',
                       'Upper Limit (Inner)',
                       'Upper Limit (Outer)',
                       ]
column_order = ['Status', 
                'Priority', 
                'Category', 
                'Tag Name', 
                'Description', 
                'Current Value', 
                'LL', 
                'L', 
                'Average (Target)', 
                'H', 
                'HH', 
                'Friendly Name']