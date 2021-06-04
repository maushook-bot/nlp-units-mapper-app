# Notes

## Units Auto Mapper

#### Business Requirements:-

The Units information from source Dataset needs to be mapped with the units from Track. Hence the folios can be mapped with the cabin Id of Track.

## Introduction:-

The Source Dataset sometimes contains the unit information in the following pattern:-

1. Unit Name Only / Short Name.
2. Unit Code Only / Short Name.
3. Both Unit Code and Unit Name.

## Configuration Inputs:-

There are two configuration inputs can be provided to config.ini based on the source dataset that falls in the category 1, 2 or 3

#### Category 1:-
If the Source Dataset contains only (Unit Name Only / Short Name) OR (Unit Code Only / Short Name) then there is no need to concat the units information in the source sql

Eg:-

- Units Mapper Parameters:-

source_sql =

     SELECT r.Folio, f.`Unit Name` AS unit_code_src,
     FROM src_hist_v12_folio_audit_report f
     JOIN src_hist_v12_reservation_made_report r ON f.`Folio Number` = r.Folio;
          
trk_sql = 

     SELECT u.unit_code, u.name, u.short_name, u.id
     FROM units u;
    
folio_select = "Folio" 

left_select = "unit_code_src"

right_select = "unit_code"



#### Category 2:-
If the Source Dataset contains (Both Unit Code and Unit Name) then the unit code and unit name needs to be  
concatenated in both source and track sql.

Eg:-

- Units Mapper Parameters:-

source_sql =

     SELECT r.Folio, CONCAT(f.`Unit Name`,' ',f.`Unit Address`) as unit_src, f.`Unit Name` AS unit_code_src,
     f.`Unit Address` AS unit_name_src FROM src_hist_v12_folio_audit_report f
     JOIN src_hist_v12_reservation_made_report r ON f.`Folio Number` = r.Folio;
          
trk_sql = 

     SELECT CONCAT(u.unit_code, ' ', u.name) AS unit_trk, u.name, u.short_name, u.unit_code, u.id
     FROM units u;
    
folio_select = "Folio" 

left_select = "unit_src"

right_select = "unit_trk"