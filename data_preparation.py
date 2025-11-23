import ast
import pandas as pd
import numpy as np  
from pyproj import Transformer


def convert_raw_data(filepath, area_filter=500, demolished=False):


    df = pd.read_csv("data/bbr_nedrivning_raw.csv", low_memory=False)
    print('Loaded raw data with records: ', len(df))
    # fix the kode list file
    kode = pd.read_excel("mappings/BBRKodelister.xlsx")
    kode['fields'] = kode['fields'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    kode = kode.explode('fields')
    # split column fields into two columns by delimiter '.'
    kode[['dataset', 'attribute']] = kode['fields'].str.split('.', expand=True)
    kode = kode[['dataset',  'attribute', 'key', 'title']]
    #kode.to_csv("bbr_kodelister_processed.csv", index=False)

    # Convert specified columns to numeric
    columns_to_convert = [
        'byg021BygningensAnvendelse',
        'byg032YdervæggensMateriale',
        'byg033Tagdækningsmateriale',
        'status',
        'byg034SupplerendeYdervæggensMateriale',
        'byg035SupplerendeTagdækningsMateriale',
        'byg026Opførelsesår'
    ]
    
    for col in columns_to_convert:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # for each column in df, replace the values with the corresponding title from kode where dataset and attribute match by the key, save the result to a new df

    # Create a copy of the original dataframe
    df_mapped = df.copy()

    # Normalize the attribute column in kode to lowercase for matching
    kode['attribute_lower'] = kode['attribute'].str.lower()

    # For each column in df
    for col in df.columns:
        # Skip non-mappable columns (like timestamps, IDs that shouldn't be mapped, etc.)
        col_lower = col.lower()
        
        # Filter kode for matching attribute
        matching_codes = kode[kode['attribute_lower'] == col_lower]
        
        if not matching_codes.empty:
            # Create a mapping dictionary from key to title
            # Convert key to numeric to match df values
            mapping_dict = {}
            for _, row in matching_codes.iterrows():
                try:
                    # Handle both integer and float keys
                    key_val = float(row['key']) if '.' in str(row['key']) else int(row['key'])
                    mapping_dict[key_val] = row['title']
                except (ValueError, TypeError):
                    # If conversion fails, use the key as-is
                    mapping_dict[row['key']] = row['title']
            
            # Apply the mapping to the column
            if mapping_dict:
                df_mapped[col] = df[col].map(mapping_dict).fillna(df[col])
                #print(f"Mapped {col}: {len(mapping_dict)} codes")
    print('Completed mapping codes to titles.')

    columnnames = pd.read_csv("mappings/columnnames.csv")
    df_mapped = df_mapped.rename(columns=columnnames.set_index('Original Field')['Translated Field'])
    print('Renamed columns to English.')
    building_usage_df = pd.read_csv("mappings/building_usage_values.csv", sep=';')

    def translate_values(column, mapping_file, original_col, translated_col, delimiter=','):
        mapping_df = pd.read_csv(mapping_file, sep=delimiter)
        mapping_dict = dict(zip(mapping_df[original_col], mapping_df[translated_col]))
        return column.replace(mapping_dict)

    # Define the translation mappings
    translation_mappings = [
        ('Building Usage Broad', 'mappings/building_usage_values.csv', 'Original Danish', 'Suggested Group', ';'),
        ('Building Usage', 'mappings/building_usage_values.csv', 'Original Danish', 'English Translation', ';'),
        ('Outer Wall Material', 'mappings/outer_wall_material.csv', 'Danish', 'English', ','),
        ('Supplementary Outer Wall Material', 'mappings/outer_wall_material.csv', 'Danish', 'English', ','),
        ('Roof Covering Material', 'mappings/roof_covering.csv', 'Danish', 'English', ','),
        ('Supplementary Roof Covering Material', 'mappings/roof_covering.csv', 'Danish', 'English', ','),
    ]

    # Apply translations
    for column, mapping_file, original_col, translated_col, delimiter in translation_mappings:
        if column == 'Building Usage Broad':
            # Create new column based on Building Usage
            df_mapped[column] = translate_values(df_mapped['Building Usage'], mapping_file, original_col, translated_col, delimiter)
        else:
            df_mapped[column] = translate_values(df_mapped[column], mapping_file, original_col, translated_col, delimiter)

    print('Translated specific column values to English.')
    # Define transformer: UTM Zone 32N (EPSG:32632) → WGS84 (EPSG:4326)
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


    # Extract coordinates from POINT format and convert from UTM to WGS84
    def convert_coordinate(coord_str):
        if pd.isna(coord_str):
            return None
        try:
            # Extract x, y from "POINT(x y)" format
            coords = coord_str.replace('POINT(', '').replace(')', '').split()
            x, y = float(coords[0]), float(coords[1])
            # Transform to WGS84 (returns lon, lat)
            lon, lat = transformer.transform(x, y)
            return f"{lat} {lon}"
        except:
            return None

    print('Converted coordinates from UTM to WGS84.')
    df_mapped['Coordinate Converted'] = df_mapped['Coordinate'].apply(convert_coordinate)
    df_mapped['lat'] = df_mapped['Coordinate Converted'].apply(lambda x: x.split()[0] if x else None)
    df_mapped['lon'] = df_mapped['Coordinate Converted'].apply(lambda x: x.split()[1] if x else None)


    ##################################
    if demolished:
        
        df_mapped['Demolition Year'] = df_mapped['Effect From'].str[:4].astype(int)
        # ignore year 1000 as it's likely a placeholder/invalid value

        df_mapped['Building Age at Demolition'] = df_mapped.apply(
            lambda row: np.nan if pd.isna(row['Year of Construction']) or row['Year of Construction'] == 1000 
            else float(row['Demolition Year']) - float(row['Year of Construction']), 
            axis=1
        )
        print('Calculated building age at demolition.')

    # Areas should be positive values
    df_mapped['Built-up Area'] = pd.to_numeric(df_mapped['Built-up Area'], errors='coerce').abs()
    df_mapped['Total Building Area'] = pd.to_numeric(df_mapped['Total Building Area'], errors='coerce').abs()
    df_mapped['Total Commercial Area'] = pd.to_numeric(df_mapped['Total Commercial Area'], errors='coerce').abs()
    df_mapped['Total Residential Area'] = pd.to_numeric(df_mapped['Total Residential Area'], errors='coerce').abs()
    print('Converted area columns to positive numeric values.')
    # new column Area that takes whatever value is bigger between 'Built-up Area' and 'Total Building Area'
    df_mapped['Area'] = df_mapped[['Built-up Area', 'Total Building Area', 'Total Commercial Area', 'Total Residential Area']].max(axis=1)
    print('Created new column Area based on maximum of area columns.')
    if area_filter > 0:
        df_mapped = df_mapped[df_mapped['Area']>=area_filter].copy()
        print(f'Filtered records with Area >= {area_filter} sqm.')

    df_mapped = df_mapped[df_mapped['Status']!='Fejlregistreret']
    
    print(f'# of records after filtering by status: \t{len(df_mapped)}')

    return df_mapped


df_nedrivning_bygning_all = convert_raw_data("data/bbr_nedrivning_bygning_all.csv", area_filter=0, demolished=False)
df_nedrivning_bygning_all.to_csv("data/bbr_nedrivning_bygning_all_mapped.csv", index=False)