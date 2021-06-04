##################################################################################
# main.py : Units Mappings between the source db and track dataset 
# Author: @@ maushook @@
# version: 2.0
##################################################################################

# Importing the Modules:-
from connections.connections import CreateConnection
from configobj import ConfigObj
from CosineUnitsMapper.units_equalizer import UnitsAutoMapper

# Config Object
conf = ConfigObj('config.ini')

# Initialize the Config Parameters:-
domain = conf['domain']
local_db_name = conf['local_db_name']

# Main Loop:-
if __name__ == '__main__':

    # Instantiate the Connection class Object:-
    con = CreateConnection(domain, local_db_name)

    # Initialize the Database Connections:-
    local = con.local_engine_connection()
    stage = con.stage_engine_connection()
    prod = con.prod_engine_connection()

    # Instantiate the Units Mapper class Object:-
    um = UnitsAutoMapper(local, stage, prod)

    # Filter the Source and Track Data sets for Mappings:-
    df_left, df_right = um.dataset_filter()

    # Call the Units Mapping Module:-
    df_mapper = um.units_mapper(df_left, df_right)

    # Extract the Cabin ID for the Mapped Df:-
    df_final, df_final_master = um.extract_cabin_id(df_mapper, df_right)

    # Load Mapped Units to Local/Stage:-
    um.load_local(df_final_master)
    um.load_stage(df_final_master)



