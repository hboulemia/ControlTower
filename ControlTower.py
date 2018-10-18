from collections import Counter
import datetime
import re
import numpy as np
import pandas as pd

class ControlTower : 
    
    """ [*] decote coherence ???     
    """
    
    def __init__(self, service_account_key = "C:/Users/N000193384/Desktop/raw_ita/Credential/ard-ita-precopromo-2018105-8987eb31c0a7.json", 
                 id_prod_col = None, id_offer_col= None, id_store_col= None, price_col= None, start_dt_offer= None, 
                 end_dt_offer= None, date_day_col = None, qty_col= None, dataframes = None, path_to_csv = None, PROMO_DATASET_FLAG = False) :
        
        self.id_prod_col = id_prod_col
        self.id_offer_col = id_offer_col
        self.price_col = price_col
        self.start_dt_offer = start_dt_offer
        self.end_dt_offer = end_dt_offer
        self.dataframes = dataframes
        self.path_to_csv = path_to_csv, 
        self.PROMO_DATASET_FLAG = PROMO_DATASET_FLAG
        self.id_store_col =  id_store_col
        self.qty_col = qty_col
        self.date_day_col = date_day_col
        self.service_account_key = service_account_key
    
    def preprocess(self, dataframes) :
        try : 
            for col in [self.start_dt_offer, self.end_dt_offer] : 
                self.dataframes[col] =  self.dataframes[col].astype("datetime64[ns]")
                self.validate_date_format(self.dataframes[col].astype("str"))
        except ValueError:
            raise ValueError("date format still incorrect, may a dateparser will be necessary")
        return dataframes
            
    def validate_date_format(self, date):
        try:
            pd.to_datetime(date, format='%Y-%m-%d', errors='raise')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")
            
    def count_mistyping_id(self) : 
        id_col = [self.id_offer_col, self.id_store_col, self.id_prod_col]
        bad_typing = 0
        for col in id_col : 
            try : 
                if not "".join(self.dataframes[col].map(str)).isdigit() :
                    bad_typing += 1
            except ValueError :  
                pass
        return (bad_typing/self.dataframes.shape[0])*100
    
    def diff_dates(self, dataframes, date1, date2) :
        if isinstance(self.dataframes[[date1, date2]],
                      datetime.date) or isinstance(self.dataframes[[self.start_dt_offer, 
                                                                    self.end_dt_offer]], datetime.datetime) :
            return pd.Series(delta.days for delta in (self.dataframes[self.end_dt_offer] - self.dataframes[self.start_dt_offer]))
        else :
            return pd.Series(delta.days for delta in 
                             (self.dataframes[self.end_dt_offer]\
                              .astype("datetime64[ns]") - self.dataframes[self.start_dt_offer]\
                              .astype("datetime64[ns]")))

    def count_incoherence_bt2w_sales_promo(self) :
        self.dataframes["nb_day_offer"] =  self.diff_dates(self.dataframes, self.start_dt_offer, self.end_dt_offer)
        vente_inf_2_over12daypromo = len([(x, y) for x, y in zip(
            self.dataframes[self.qty_col], self.dataframes["nb_day_offer"]) if x < 2 and y > 9])
        return (vente_inf_2_over12daypromo/self.dataframes.shape[0])*100

    def returns_null_values(self) :
        nul = pd.DataFrame(self.dataframes.isnull().sum(), self.dataframes.columns)
        nul0 = nul[nul != 0].dropna().rename(columns = {0 : "missing_values"})
        nul0["%"] = nul[0].apply(lambda x : x/len(df))
        return nul0
    
    def count_incoherent_offer_date(self) :
        """ if the date of begin_offer is > end offer"""
        try : 
            for col in [self.start_dt_offer, self.end_dt_offer] : 
                self.dataframes[col] =  self.dataframes[col].astype("datetime64[ns]")
                self.validate_date_format(self.dataframes[col].astype("str"))
            bad_date = [(x,y) for x,y in zip(self.dataframes[self.start_dt_offer], self.dataframes[self.end_dt_offer]) if x > y]
            return (len(bad_date)/self.dataframes.shape[0])*100
        except ValueError:
            raise ValueError("date format still incorrect, may a dateparser will be necessary")
            
    def count_bad_date_format(self) :
        date_list = [self.start_dt_offer, self.end_dt_offer]
        count_good_date = 0
        #b = count_bad_date_format(df, "dts_validity_offer")
        for date in date_list : 
            try :
                datetime.datetime.strptime(date, '%Y-%m-%d')
                count_good_date +=1
            except ValueError :
                pass
        len_of_list = len(date_list)
        bad_date = len_of_list - count_good_date
        return (bad_date/self.dataframes.shape[0])*100
    
    def count_negatif_values(self) : 
        bad_price = 0 
        bad_stock = 0
        
        for qty in df[self.qty_col].values : 
            if qty < 0 :
                bad_stock += 1
            else :
            bad_stock = bad_stock
        
        for price in df[self.price_col].values :
            if price <0 :
                bad_price += 1
            else :
                bad_price = bad_price
                
        return (bad_price/self.dataframes.shape[0])*100, (bad_stock/self.dataframes.shape[0])*100
    
    def count_duplicates(self) :     
        c = Counter(list(zip(self.dataframes[self.id_store_col], 
                             self.dataframes[self.id_prod_col], self.dataframes[self.id_offer_col])))
        is_duplicates = len(([k for k, v in c.items()  if v >= 2]))
        return (is_duplicates/self.dataframes.shape[0])*100
    
    def count_at_least_one_sales_per_day(self) : 
        c = Counter(list(zip(self.dataframes[self.id_store_col], 
                             self.dataframes[self.id_prod_col], self.dataframes[self.date_day_col])))
        no_historical = len(([k for k, v in c.items()  if v < 1]))
        return (no_historical/self.dataframes.shape[0])*100
    
    def count_promo_duplicates(self) : 
        c = Counter(list(zip(self.dataframes[self.id_store_col], self.dataframes[self.id_offer_col],
                             self.dataframes[self.id_prod_col], self.dataframes[self.date_day_col])))
        promo_dup = len(([k for k, v in c.items() if v > 1]))
        return (promo_dup/self.dataframes.shape[0])*100
    
    def incoherent_price(self) : 
        promo_suptonormal = 0
        equality = 0
        self.dataframes["flagcol"] = np.where(self.dataframes[self.id_offer_col].isnull(), 0, 1)
        for x, y in zip(self.dataframes.loc[self.dataframes["flagcol"] == 1][self.price_col], 
                        self.dataframes[self.price_col]) :
            #prix avc promo vs sans promo
            if x > 0 :
                promo_suptonormal += 1
            else : 
                promo_suptonormal = promo_suptonormal

            if x == y :
                equality += 1
            else : 
                equality=equality

        is_sup = (promo_suptonormal/self.dataframes.shape[0])*100
        is_equal = (equality/self.dataframes.shape[0])*100
        return is_sup, is_equal
    
    def warning_table(self) :
        self.dataframes = self.preprocess(self.dataframes)
        if self.PROMO_DATASET_FLAG == True : 
            return pd.DataFrame({
                "is_duplicate" : self.count_duplicates(), 
                "is_ids_mistyping" : self.count_mistyping_id(), 
                "is_incoherent_offerdt" : self.count_incoherent_offer_date(), 
                "is_bad_formated_date" : self.count_bad_date_format(), 
                "is sup_topromo": self.incoherent_price()[0], 
                "is_equal_topromo" : self.incoherent_price()[1],
                "is_incoherent_sales_in_promo" : self.count_incoherence_bt2w_sales_promo()
            }).rename(columns = {0 : "in %"})
        else : 
            return pd.DataFrame.from_dict({
                "is_duplicate" : self.count_duplicates(), 
                "is_ids_mistyping" : self.count_mistyping_id(), 
                "is_bad_formated_date" : self.count_bad_date_format()}, orient='index').rename(columns = {0 : "in %"})
        
       
    def pretty_table(self) :
        base_df =  self.warning_table()
        trace = go.Table(
            header=dict(values=list(base_df.columns),
                        fill = dict(color='#C2D4FF'),
                        align = ['left'] * 5),
            cells=dict(values=[base_df[col] for col in base_df.columns],
                       fill = dict(color='#F5F8FF'),
                       align = ['left'] * 5))
        data = [trace] 
        return iplot(data, filename = 'warning_table_png')