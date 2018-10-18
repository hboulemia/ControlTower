# ControlTower

# How to use : 

* init parameters through class

      CT = ControlTower(id_prod_col = "id_sku_x", id_offer_col = "id_offer", id_store_col = "id_store", price_col = "prc_sku", 
                        start_dt_offer = "DP_DT_INIZ_VALD",qty_col = "sold_quantities", end_dt_offer = "DP_DT_FINE_VALD"
                        , dataframes= merge, path_to_csv = None, 
                        PROMO_DATASET_FLAG = True)
                        
                        
 * tcheck metrics one by one : 
 
       CT.returns_null_values() #it works
       CT.count_incoherent_offer_date() #it works
       CT.count_duplicates() #it works
       CT.count_incoherence_bt2w_sales_promo()
       CT.count_negatif_values()
       
 * build a table with everything :
 
        CT.warning_table()
        
 * upload a prettiest png of warning_table : (in construction)
 
         CT.pretty_table()

... Doc in Progress
