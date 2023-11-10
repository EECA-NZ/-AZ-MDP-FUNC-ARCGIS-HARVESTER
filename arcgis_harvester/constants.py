"""
This module has the constants that are needed to configure the funcion
"""

CONTAINER_NAME = "bill-nelson-dev"
CSV_FILE_PATH_PREFIX = "file-drop/ArcGIS"
LAYERS_TO_IMPORT = [
    # RETA map GXP sites
    ("https://services6.arcgis.com/WXm4pYrosIXLR8iW/ArcGIS/rest/services/"
     "EECA_Migrate_WFL1/FeatureServer/5"),
    # Transpower Public GXP sites
    ("https://services3.arcgis.com/AkUq3zcWf7TVqyR9/arcgis/rest/services/"
     "Sites/FeatureServer/0"),
]
