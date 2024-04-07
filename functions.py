from uszipcode import SearchEngine
import pandas as pd
import folium
from xml.etree import ElementTree as ET
import requests
import prettyprinter as pp
import xml

'''
The purpose of this function is to put all of our functions into 1 to reduce time to
code up and process data
'''

def black_box(data, minimum_e, save_excel, save_map, river_c=[]):  

    # HERE WE ARE NOW MODIFYING PROCESSING OUR DATAFRAME
    # setting up the counties we want to focus on
    river_counties = county_concat(river_c)

    # first we will clean the county values
    df = clean_county_val(data)

    # insert locations column
    df = insert_location(df)

    # if there are counties given then process, else continue without processing any
    if len(river_c) > 0:
        # filter our df
        df = filter_df(df, minimum_e, counties=river_counties)

    else: 
        df = filter_df(df, minimum_e)

    # adding the needed empty columns in our dataframe
    # these columns will be populated with data from meander_xml function
    df['Unit Type'] = ''
    df['Parent Company'] = ''
    df['Fuel Type'] = ''   


    for index, row in df.iterrows():

        '''
        purpose is to go through, grab the 3 values and add them to the dataframe
        '''

        # defining our ghgp_id from the row we just grabbed
        ghgp = row['facility_id']
        print(ghgp)

        # now we will be grabbing and adding the 3 values to our datframe
        # adding a unit type value
        df.loc[index, 'Unit Type'] = meander_xml(ghgp_id=ghgp, topic='unit type')

        # adding a parent company value
        df.loc[index, 'Parent Company'] = meander_xml(ghgp_id=ghgp, topic='parent company')

        # adding a fuel type value
        df.loc[index, 'Fuel Type'] = meander_xml(ghgp_id=ghgp, topic='fuel type')


    # output as an excel file
    df.to_excel(save_excel, index=False)

    # now we want to map our data
    if len(save_map) > 0: 
        '''
        if there is a value in save_map then we should start process of passing
        this function 
        '''
        map_it(df, save_map)
        print('succesfully created map :3')
    
    else: 
        print('no map created :/')



'''
The purpose of this file is to return a coutny when given a zipcode value. 

Specifically, we will be working with an EPA Emissions data file for 2021 that provides us with emissions data for the united states. 

The main issue is that the data is incomplete in terms of county values. 
Thus, we need to go into each worksheet and fill in the county values by using the 
reported zipcodes that are included...

'''

def get_county_from_zip(zipcode):
    search = SearchEngine()
    result = search.by_zipcode(zipcode)
    if result:
        return result.county
    else:
        return None


'''
The purpose of this function is to iterate through each row of a dataframe and add a county value if there is not one already there

'''

def insert_location(dataframe):

    for index, row in dataframe.iterrows():

        # check if County column is empty 
        county_value = row['County']

        if pd.isna(county_value) or county_value == '':

            # 
            dataframe.at[index, 'County'] = get_county_from_zip(row['Zip Code'])

        else:

            # if not empty, then just leave alone
            continue

    # now let's add a new column to our dataframe that adds the County and the State
    dataframe['Location'] = dataframe['County'] + ', ' + dataframe['State']

    return dataframe


'''
The purpose of this function is to iterate through each row in our dfs and remove the word County from the County column value

'''

def clean_county_val(dataframe):

    # these are the words that we want to remove
    words_to_remove = ['County', 'county', 'COUNTY']

    # regular expression pattern to match any of the words 
    pattern = "|".join(words_to_remove)

    # removing the words from the column value 
    dataframe['County'] = dataframe['County'].str.replace(pattern, '', regex=True)

    # making all County names uppercase
    dataframe['County'] = dataframe['County'].str.upper()


    return dataframe


'''
The purpose of this code is to select a minimum emissions standard and remove any emitters that do not meet this threshold

'''

def filter_df(dataframe, minimum_e, counties=[]):

    # filters out any rows with values that do not meed the minimum threshold
    try:
        # Try accessing column with the first name
        dataframe = dataframe[dataframe['GHG QUANTITY (METRIC TONS CO2e)'] >= minimum_e]

    except KeyError:
        
        try:
            # If the first column name is not found, try the second name
            dataframe = dataframe[dataframe['Total Reported Emissions'] >= minimum_e]
        
        except KeyError:
            # If neither column name is found, handle the error or raise an exception
            print("Both column names not found!")    

    # filters out by counties you are interested in looking at
    # for it to work, you need to provide a counties list, else it ignores it
    if len(counties) > 0: 

        # then run process of filtering our by list of counties
        dataframe = dataframe[dataframe['Location'].isin(counties)]

        return dataframe

    else: 

        # in essence, just continue
        return dataframe


'''
The purpose of this function is to create a map with the long, and lat values in dfs

'''

def format_number(x):

    """Format the number with commas and no decimal points."""
    return "{:,.0f}".format(x)


def map_it(dataframe, save_path, color='red', size=(10,10), tiles="OpenStreetMap"):

    # here we want to create a dictionary of type of facilities to color our markers
    facility_color = {
        'Power Plants': 'green'
    }

    # let's convert some of the column values into values with commas and no decimals
    dataframe["Total Reported Emissions"] = dataframe["Total Reported Emissions"].apply(format_number)
    dataframe["CO2 emissions (non-biogenic) "] = dataframe["CO2 emissions (non-biogenic) "].apply(format_number)


    # create a map centered on the USA
    map = folium.Map(location = [39.8283, -98.5795], # center of map 
                     zoom_start=  4, # where you want to start in zoom
                     tiles = tiles) # map type
    

    # iterating through our df to add markers
    for index, row in dataframe.iterrows():

        #Setup the content of the popup
        iframe = folium.IFrame(f'Facility Name: {str(row["FACILITY NAME"])} <br> Industry Type: {str(row["Industry Type (sectors)"])} <br> Total Reported Emissions: {str(row["Total Reported Emissions"])} <br> CO2 Emissions: {str(row["CO2 emissions (non-biogenic) "])} <br> Parent Company: {str(row["Parent Company"])} <br> Unit Type: {str(row["Unit Type"])} <br> Fuel Type: {str(row["Fuel Type"])}')
    
        #Initialise the popup using the iframe
        popup = folium.Popup(iframe, min_width=300, max_width=300)
    
        try:
            icon_color = facility_color[row['Industry Type (sectors)']]
        
        except:
            #Catch nans
            icon_color = 'gray'

        folium.Marker(
            location = [row['LATITUDE'], row['LONGITUDE']],
            icon = folium.Icon(
            color = icon_color,
            icon = '',
            shadow = None,
            size = size,
            ),
            popup = popup
        ).add_to(map)


    # now we are saving our map given the save path
    map.save(save_path)


'''
The purpose of this function is to parse out the first and second index values of a
county, state array.

Moreover, this function will concatenate the values and create a new column:

for instance, say you have ['Benton', 'AR']...

function will concatenate and create a new column--> 'Benton, AR'
'''

def county_concat(county_list): 

    # let's create an empty list to store our concatenated counties list
    empty_list = []

    for county in county_list:

        # we want to concatenate
        concat = county[0].upper() + ", " + county[1].upper()
        empty_list.append(concat)

    # now we just return the newly list that we created
    return empty_list


'''
The purpose of this function is extract and process an XML file.

Not entirely sure what an XML file is, or how it compares to a JSON file but it seems
very low memory and has a tree structure.

This is how facilities provide reports to the EPA emissions entity. 

Please note that you need to:

'pip install elementpath' and then import the module into your env

to import: 
'xml.etree.ElementTree as ET'

'''

def meander_xml(ghgp_id, topic, year=2021):

    '''
    Given a ghgp_id, return the fuel type within the xml code.
    Please note that ghgp_id and facility_id are equivalent in terms of input. 
    '''

    try:

        # here, we will be adding the ghgp_id value to a url used to make GET request
        url = f'https://ghgdata.epa.gov/ghgp/service/xml/{year}?id={ghgp_id}&et=undefined'

        # making the url GET request for the xml file
        response = requests.get(url=url)
        webpage_content = response.text

        # modifying our url contnet because it containts code we do not need
        lines = webpage_content[38:-6].split('\n')
        modified_content = '\n'.join(lines[1:])

        # parse the XML
        root = ET.fromstring(modified_content)

        # now we want to create an ifelse statement that will grab specific data
        # user needs

        if topic == 'Fuel Type' or topic == 'fuel type':

            # creating a list to return the found elements to
            fuel_types = []

            # extract the fuel type element
            for fuel_type in root.findall(".//FuelType"):

                # return the fuel_type value
                fuel_types.append(fuel_type.text)
            
            # removing redundancies
            fuel_types = list(set(fuel_types))

            # concatenating our list to output a long string
            fuel_typez = ", ".join([str(item) for item in fuel_types])

            return fuel_typez
    

        elif topic == 'Unit Type' or topic == 'unit type':
    
            # creating a list to return the found elements to
            unit_types = []

            # extract the unit type value
            for unit_type in root.findall('.//UnitType'):

                # return the unit_type value
                unit_types.append(unit_type.text)
            
            # removing redundancies
            unit_types = list(set(unit_types))

            # concatenating our list to output a long string
            unit_typez = ", ".join([str(item) for item in unit_types])

            return unit_typez
    

        elif topic =='Parent Company' or topic == 'parent company': 
        
            # creating a list to return the found elements to
            parent_cos = []
            
            # else, extract parent company name 
            for parent_co in root.findall('.//ParentCompanyLegalName'):

                # return the parent company name
                parent_cos.append(parent_co.text)
            
            # removing redundancies
            parent_cos = list(set(parent_cos))

            # concatenating our list to output a long string
            parent_coz = ", ".join([str(item) for item in parent_cos])

            return parent_coz
    

        else: 

            # means none of the 3 topics were given 
            print('Error: please insert a valid topic to receive a valid value!')


        # now we add this fuel type value to a new column called "Fuel Type"
        # dataframe['Fuel Type'] = str(fuel_type)


    except xml.etree.ElementTree.ParseError:
        # xml file is not properly formatted, so skip this iteration
        "sorry, bud!"
        pass



'''
Purpose of this code is to grab all the relevant values and add the new values in the dataframe
'''

def add_xml_bits(dataframe, ghgp_id):

    # adding a unit type value
    dataframe['Unit Type'] = meander_xml(ghgp_id, topic='unit type')

    # adding a parent company value
    dataframe['Parent Company'] = meander_xml(ghgp_id, topic='parent company')

    # adding a fuel type value
    dataframe['Fuel Type'] = meander_xml(ghgp_id, topic='fuel type')


