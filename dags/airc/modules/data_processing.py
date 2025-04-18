import pandas as pd
import numpy as np
from io import BytesIO
import json
import io
from typing import List, Dict

from airc.modules.airflow_connections import _get_minio_connection
from airc.modules.utlis import get_unique_values, check_keywords, check_type, check_view

"""
#TODO
Có thể define dưới dạng 1 class
Class này sẽ define các thông tin của 1 bucket
- bucket-name
- Object path, raw, intermediate, processed,...
- client connection
- secret (class sẽ tự retrieve từ đâu đó)
- password (class sẽ tự retrieve từ đâu đó)
- Có thể code theo hướng thừa kế, 1 parent class định nghĩa các
functions phải có khi làm việc với MinIO
"""

def minio_object_to_dataframe(minio_object):
    """_summary_

    Args:
        minio_object (_type_): _description_

    Returns:
        _type_: _description_
    """
    df = pd.read_csv(io.BytesIO(minio_object.read()), low_memory=False)
    return df

def minio_object_to_dataframe_with_column(minio_object, columns_to_read: List):
    df = pd.read_csv(io.BytesIO(minio_object.read()), 
                    usecols=columns_to_read,
                    low_memory=False)
    return df

def drop_duplicate_row(df: pd.DataFrame):
    return df.drop_duplicates()

def drop_duplicate_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=1, how='all')

def drop_null(df: pd.DataFrame, column_to_drop: List[str]) -> pd.DataFrame:
    return df.dropna(subset=column_to_drop)

def get_minio_object(BUCKET_NAME, OBJECT_PATH):
    minio_client = _get_minio_connection()
    response = minio_client.get_object(bucket_name=BUCKET_NAME,
                      object_name=OBJECT_PATH)
    return response

def put_object_to_minio(client, put_object, BUCKET_NAME, OBJECT_PATH):
    client.put_object(
        BUCKET_NAME, OBJECT_PATH, io.BytesIO(put_object), 5,
        )
    return 0

def put_file_to_minio(BUCKET_NAME, OBJECT_PATH, OUTPUT_FILE_PATH):
    minio_client = _get_minio_connection()
    minio_client.fput_object(bucket_name=BUCKET_NAME,
                            object_name=OBJECT_PATH,
                            file_path=OUTPUT_FILE_PATH,
                            content_type="application/csv")
    return 0

def list_minio_object(client, BUCKET_NAME, OBJECT_PATH):
    response = client.list_objects(
        bucket_name=BUCKET_NAME,
        prefix=f"{OBJECT_PATH}/",
        # recursive=True
    )  # ['ResponseMetadata', 'IsTruncated', 'Contents', 'Name', 'Prefix', 'MaxKeys', 'EncodingType', 'KeyCount']

    csv_files = []
    for obj in response:
        serialize_obj = json.loads(json.dumps(
            obj.__dict__, indent=4, default=str))
        prefix = serialize_obj['_object_name']
        file_path = f"s3://{BUCKET_NAME}/{prefix}"
        csv_files.append(file_path)
    return csv_files

def concatenate_files(BUCKET_NAME: str, OBJECT_PATH: str, axis=0) -> pd.DataFrame:
    minio_client = _get_minio_connection()
    file_paths = list_minio_object(minio_client, BUCKET_NAME, OBJECT_PATH)
    
    dfs = []
    for path in file_paths:
        path_parts = path.replace("s3://", "").split("/", 1)
        bucket_name = path_parts[0]
        object_name = path_parts[1]
        response = minio_client.get_object(bucket_name, object_name)
        df  = minio_object_to_dataframe(response)
        dfs.append(df)
    print(file_paths)
    if axis==0:
        final_df = pd.concat(dfs, ignore_index=False)
    elif axis==1:
        final_df = pd.concat(dfs, ignore_index=False, axis=1)
    
    print(final_df.columns)
    return final_df

def exclude_unused_data(df):
    df = drop_duplicate_row(df)
    df = drop_duplicate_column(df)
    null_column_to_drop = ["streetAddress", "addressLocality", "addressRegion"]
    df = drop_null(df, column_to_drop=null_column_to_drop)
    return df

def process_acreage(minio_object):
    acreage_df = minio_object_to_dataframe_with_column(minio_object, ["Acreage"])
    acreage_df["Acreage"].fillna(0, inplace=True)
    return acreage_df

def process_garage(minio_object) -> pd.DataFrame:
    garage_df = minio_object_to_dataframe_with_column(minio_object, ["Garage", "Parking", "Features"])
    unique_features = get_unique_values(garage_df["Features"])
    # unique_garage_features = get_unique_values(garage_df["Garage"])
    unique_parking_features = get_unique_values(garage_df["Parking"]) | unique_features
    
    garage_keywords = [
        "garage", "attached", "detached", "single", 
        "double", "triple", "quad", "heated", "rv", 
        "oversized", "underground", "tandem", "car"
        ]
    garage_list = []
    for item in unique_parking_features:
        lower_item = item.lower()
        if any(keyword in lower_item for keyword in garage_keywords):
            garage_list.append(item.lower())
    garage_list.remove("no garage")
    garage_df["Garage_new"] = garage_df["Garage"]
    garage_df.loc[garage_df.Garage.isna(), "Garage_new"] = garage_df.loc[garage_df.Garage.isna(), "Features"].apply(lambda x: check_keywords(x, garage_list))
    garage_df.drop(columns=["Garage"], axis=1, inplace=True)
    return  garage_df

def transform_basement(minio_object):
    basement_df = minio_object_to_dataframe_with_column(minio_object, ["Basement"])
    
    basement_types = {
        'Finished': ['full basement', 'full', 'dugout', 'fully finished', 'finished', 'remodeled basement', 
                    'apartment in basement', 'suite', 'walk-out access', 'walk-out to grade', 'walk-up to grade', 
                    'walk-out', 'walkout', 'walk-up', 'with windows', 'separate/exterior entry', 'separate entrance'], 
        'Partial': ['partially finished', 'partial', 'partially finished', 'partial basement', 
                    'not full height', 'cellar'], 
        'No basement': ['unfinished', 'no basement', 'slab', 'crawl space', 'crawl', 'none', 
                        'not applicable', 'n/a']}
    basement_df['Basement_new'] = basement_df['Basement']
    basement_df['Basement_new'] = basement_df['Basement_new'].apply(lambda x: check_type(x, basement_types))
    basement_df.drop(columns=["Basement"], axis=1, inplace=True)
    return basement_df

def transform_exterior(minio_object):
    exterior_df = minio_object_to_dataframe_with_column(minio_object, ["Exterior"])
    
    exterior_materials = {
        'Metal': ['aluminum', 'aluminum siding', 'aluminum/vinyl', 'colour loc', 'metal', 'steel'],
        'Brick': ['brick', 'brick facing', 'brick imitation', 'brick veneer'],
        'Concrete': ['concrete', 'concrete siding', 'concrete/stucco', 'concrete block', 'insul brick', 'stucco'],
        'Wood': ['cedar', 'cedar shingles', 'cedar siding', 'wood', 'wood siding', 'wood shingles', 'wood shingles', 'wood siding'],
        'Composite': ['composite siding', 'hardboard', 'masonite', 'shingles'],
        'Vinyl': ['vinyl', 'vinyl siding', 'asbestos', 'siding']
        }
    exterior_df['Exterior_new'] = exterior_df['Exterior']
    exterior_df['Exterior_new'] = exterior_df['Exterior_new'].apply(lambda x: check_type(x, exterior_materials))
    exterior_df.drop(columns=["Exterior"], axis=1, inplace=True)
    return exterior_df

def transform_fireplace(minio_object):
    fireplace_df = minio_object_to_dataframe_with_column(minio_object, ["Fireplace"])
    
    fireplace_df['Fireplace_new'] = fireplace_df['Fireplace']
    fireplace_df.loc[fireplace_df['Fireplace_new'].isin(['0', '[]','["0"]']), 'Fireplace_new'] = np.nan
    fireplace_df.loc[fireplace_df['Fireplace_new'].notna(), 'Fireplace_new'] = 'Yes'
    fireplace_df.loc[~fireplace_df['Fireplace_new'].notna(), 'Fireplace_new'] = 'No'
    fireplace_df.drop(columns=["Fireplace"], axis=1, inplace=True)
    return fireplace_df

def transform_heating(minio_object):
    heating_df = minio_object_to_dataframe_with_column(minio_object, ["Heating"])
    
    heating_categories = {
        'forced air': ['forced air', 'forced air-1', 'forced air-2', 'furnace'],
        'boiler': ['boiler', 'hot water', 'hot water radiator heat', 'steam', 'steam radiator'],
        'heat pump': ['central heat pump', 'heat pump', 'wall mounted heat pump'],
        'radiant': ['radiant', 'radiant heat', 'radiant ceiling', 'radiant floor', 
                    'radiant/infra-red heat', 'baseboard', 'baseboard heaters', 'electric baseboard units'],
        'fireplace': ['fireplace(s)', 'fireplace insert', 'wood stove', 'pellet stove', 'coal stove', 'stove'],
        'space heat': ['space heater', 'space heaters', 'wall furnace', 'floor furnace', 
                    'floor model furnace', 'overhead heaters', 'overhead unit heater', 'ductless'],
        'alt heat': ['geo thermal', 'geothermal', 'solar', 'gravity', 'gravity heat system', 
                    'oil', 'propane', 'propane gas', 'coal'],
        'no heat': ['no heat', 'none'],  # only contains no heat options
        'other': ['mixed', 'combination', 'sep. hvac units'],  # these suggest non-specific or mixed systems
        }
    
    heating_df['Heating_new'] = heating_df['Heating']
    heating_df['Heating_new'] = heating_df['Heating_new'].apply(lambda x: check_type(x, heating_categories))
    heating_df.drop(columns=["Heating"], axis=1, inplace=True)
    return heating_df

def transform_flooring(minio_object):
    flooring_df = minio_object_to_dataframe_with_column(minio_object, ["Flooring"])
    flooring_categories = {
        'carpet': ['carpet', 'carpet over softwood', 'carpet over hardwood', 'carpeted', 'wall to wall carpet', 
                'wall-to-wall carpet'],
        'wood': ['bamboo', 'engineered wood', 'engineered hardwood', 'hardwood', 'parquet', 'softwood', 'wood'],
        'tile': ['ceramic', 'ceramic tile', 'ceramic/porcelain', 'porcelain tile', 'non-ceramic tile', 'slate', 
                'stone', 'tile'],
        'vinyl': ['cushion/lino/vinyl', 'vinyl', 'vinyl plank'],
        'laminate': ['laminate', 'laminate flooring'],
        'concrete': ['concrete', 'concrete slab'],
        'other': ['basement slab', 'basement sub-floor', 'granite', 'heavy loading', 'linoleum', 'marble', 'mixed', 
                'mixed flooring', 'see remarks', 'subfloor', 'other']
        }
    #TODO Nhớ đổi tên cột
    flooring_df['Flooring_new'] = flooring_df['Flooring']
    flooring_df['Flooring_new'] = flooring_df['Flooring_new'].apply(lambda x: check_type(x, flooring_categories))

    flooring_df.drop(columns=["Flooring"], axis=1, inplace=True)
    return flooring_df

def transform_roof(minio_object):
    roof_df = minio_object_to_dataframe_with_column(minio_object, ["Roof"])
    roofing_categories = {
        'asphalt': ['asphalt', 'asphalt & gravel', 'asphalt rolled', 'asphalt shingle', 'asphalt shingles', 'asphalt torch on', 'asphalt shingle', 'asphalt/gravel'],
        'cedar shake': ['cedar shake', 'cedar shakes'],
        'clay': ['clay tile'],
        'concrete': ['concrete', 'concrete tiles'],
        'fiberglass': ['fiberglass', 'fiberglass shingles', 'fibreglass shingle'],
        'flat': ['flat', 'flat torch membrane', 'membrane', 'epdm membrane'],
        'metal': ['metal', 'metal shingles', 'steel', 'tin'],
        'pine shake': ['pine shake', 'pine shakes'],
        'rubber': ['rubber'],
        'sbs': ['sbs roofing system'],
        'shake': ['shake'],
        'shingle': ['shingle', 'vinyl shingles'],
        'slate': ['slate'],
        'tar': ['tar & gravel', 'tar & gravel', 'tar &amp; gravel', 'tar/gravel'],
        'tile': ['tile'],
        'wood': ['wood', 'wood shingle', 'wood shingles'],
        'other': ['conventional', 'mixed', 'other'],
        }
    roof_df['Roof_new'] = roof_df['Roof']
    roof_df['Roof_new'] = roof_df['Roof_new'].apply(lambda x: check_type(x, roofing_categories))
    roof_df.drop(columns=["Roof"], axis=1, inplace=True)
    return roof_df

def transform_waterfront(minio_object):
    waterfront_df = minio_object_to_dataframe_with_column(minio_object, ["Waterfront"])

    waterfront_df['Waterfront_new'] = waterfront_df['Waterfront']
    waterfront_df.loc[waterfront_df['Waterfront_new'].isna(), 'Waterfront_new'] = 'No'
    waterfront_df.drop(columns=["Waterfront"], axis=1, inplace=True)
    return waterfront_df

def transform_sewer(minio_object):
    sewer_df = minio_object_to_dataframe_with_column(minio_object, ["Sewer"])
    sewage_categories = {
        'municipal': ['municipal/community', 'municipal sewage system', 'sanitary sewer', 'sewer', 'sewer connected', 
                    'sewer to lot', 'sewer available', 'public sewer', 'attached to municipal'],
        'septic': ['septic tank', 'septic system', 'septic system: common', 'septic field', 'septic tank and field', 
                'septic tank & mound', 'mound septic', 'septic tank & field', 'septic needed', 'engineered septic'],
        'private': ['private sewer', 'private sewer', 'holding tank', 'low pressure sewage sys', 'shared septic'],
        'alternative': ['aerobic treatment system', 'facultative lagoon', 'lagoon', 'outflow tank', 'open discharge', 
                        'liquid surface dis', 'pump', 'tank & straight discharge'],
        'none': ['no sewage system', 'outhouse', 'none'],
        }
    sewer_df['Sewer_new'] = sewer_df['Sewer']
    sewer_df['Sewer_new'] = sewer_df['Sewer_new'].apply(lambda x: check_type(x, sewage_categories))
    sewer_df['Sewer_new'] = sewer_df['Sewer_new'].fillna('none')
    sewer_df.drop(columns=["Sewer"], axis=1, inplace=True)
    return sewer_df

def add_new_features(minio_object):
    """Cái này sẽ là additional input cho các task: pool, 

    Args:
        minio_object (_type_): _description_

    Returns:
        _type_: _description_
    """
    mix_features = ['Basement', 'Exterior', 'Features', 'Fireplace', 'Garage', 'Heating', 'Parking']
    new_featuers_df = minio_object_to_dataframe_with_column(minio_object, mix_features)
    new_featuers_df['Combined'] = new_featuers_df[mix_features].astype(str).agg(','.join, axis=1)
    return new_featuers_df

def add_pool(minio_object):# Thêm combine sau
    pool_df = minio_object_to_dataframe_with_column(minio_object, ["Combined"])
    pool_features = ["swimming pool", "public swimming pool"]
    pool_df['Pool'] = pool_df['Combined'].apply(lambda x: check_keywords(x, pool_features))
    pool_df.drop(columns=["Combined"], axis=1, inplace=True)
    return pool_df

def add_garden(minio_object): # Thêm combine sau
    garden_df = minio_object_to_dataframe_with_column(minio_object, ["Combined"])
    garden_features = [
                    "vegetable garden", "garden", 
                    "fruit trees/shrubs", "private yard", 
                    "partially landscaped", "landscaped"
                    ]
    garden_df['Garden'] = garden_df['Combined'].apply(lambda x: check_keywords(x, garden_features))
    garden_df.drop(columns=["Combined"], axis=1, inplace=True)
    return garden_df

def add_view(minio_object):# Thêm combine sau
    view_df = minio_object_to_dataframe_with_column(minio_object, ["Combined"])
    view_features = ["View Downtown", "River View", "View City", "View Lake", "Lake View", 
                    "Ravine View", "River Valley View"]
    view_features = [f.lower() for f in view_features]
    view_df['View'] = view_df['Combined'].apply(lambda x: check_view(x, view_features))
    view_df.loc[view_df['View'].isin(['view lake', 'lake view']), 'View'] = 'Lake'
    view_df.loc[view_df['View'].isin(['view downtown']), 'View'] = 'Downtown'
    view_df.loc[view_df['View'].isin(['view city']), 'View'] = 'City'
    view_df.loc[view_df['View'].isin(['river view']), 'View'] = 'River'
    view_df.loc[view_df['View'].isin(['ravine view', 'river valley view']), 'View'] = 'Valley'
    view_df.drop(columns=["Combined"], axis=1, inplace=True)
    return view_df

def add_balcony(minio_object):# Thêm combine sau
    balcony_df = minio_object_to_dataframe_with_column(minio_object, ["Combined"])
    balcony_features = ['Balcony', 'Balcony/Deck', 'Balcony/Patio']
    balcony_features = [f.lower() for f in balcony_features]
    balcony_df['Balcony'] = balcony_df['Combined'].apply(lambda x: check_keywords(x, balcony_features))
    balcony_df.drop(columns=["Combined"], axis=1, inplace=True)
    return balcony_df

def join_data(BUCKET_NAME, OBJECT_PATH):
    concatenate_df = concatenate_files(BUCKET_NAME, OBJECT_PATH, axis=1)
    return concatenate_df

def drop_old_columns(df: pd.DataFrame):
    """_summary_

    Args:
        df (pd.DataFrame): _description_

    Returns:
        _type_: _description_
    """    
    df = df.drop(columns=['streetAddress', 'postalCode', 'description', 'priceCurrency', 
                        "Parking Features", 'Basement', 'Exterior', 'Features', 'Fireplace', 
                        'Garage', 'Heating', 'MLS® #', 'Roof', 'Sewer', 'Waterfront', 
                        'Parking', 'Flooring', 'Fireplace Features', 'Subdivision', 
                        'Square Footage', 'Bath', 'Property Tax'], axis=1)
    
    
    return df

def rename_column(df):
    """
    Generates a dictionary mapping old column names to new ones based on a predefined renaming rule.

    Args:
        columns_list (List[str]): A list of original column names.

    Returns:
        Dict[str, str]: A dictionary where keys are original column names and values are the renamed versions.
    """
        #TODO Nhớ check tên cột

    df = df.rename(columns={'addressLocality': 'City', 
                            'addressRegion': 'Province', 
                            'latitude': 'Latitude', 
                            'longitude': 'Longitude', 
                            'price': 'Price',
                            'property-baths': 'Bathrooms', 
                            'property-beds': 'Bedrooms', 
                            'property-sqft': 'Square_Footage', 
                            'Garage new': 'Garage',	
                            'Parking_new': 'Parking',	
                            'Basement_new': 'Basement', 
                            'Exterior_new': 'Exterior', 
                            'Fireplace_new': 'Fireplace', 
                            'Heating_new': 'Heating', 
                            'Flooring_new': 'Flooring', 
                            'Roof_new': 'Roof', 
                            'Waterfront_new': 'Waterfront', 
                            'Sewer_new': 'Sewer'})
    return df

