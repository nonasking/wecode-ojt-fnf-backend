import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql
import openpyxl

from django.http import JsonResponse
from django.views import View

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday

class CategoryTreeView(View):

    def get_query(self, query, *args, **kwargs):
        query = query.format(
            brand=kwargs["brand"],
            adult_kids=kwargs["adult_kids"],
        )
        return query
    
    def get_categories(self, brand, adult_kids, connect):
        query = """
        
select distinct value
from (
         select distinct cat_nm as value
         from prcs.db_prdt
         where brd_cd = '{brand}'
           and adult_kids_nm = '{adult_kids}'
           and cat_nm != 'TBA'
           and ord_qty != 0
--         union all
--         select distinct cat_nm as value
--         from prcs.db_srch_kwd_naver_mst
--         where brd_cd = '{brand}'
--           and adult_kids = '{adult_kids}'
--           and comp_type != '라이프스타일'
--           and cat_nm != '일반'
     ) a
order by 1        

        """
        categories_query = self.get_query(
            query = query,
            brand = brand,
            adult_kids = adult_kids,
        )

        categories_redshift_data = RedshiftData(connect, categories_query)
        categories_data = categories_redshift_data.get_data()

        categories_list = categories_data.values.tolist()

        result = []
        for category in categories_list:
            result += category
                
        return result
    
    def get_subcategories(self, brand, adult_kids, connect):
        query = """

select distinct sub_cat_nm as value, cat_nm as parent_value
from (
         select distinct cat_nm, sub_cat_nm
         from prcs.db_prdt
         where brd_cd = '{brand}'
           and adult_kids_nm = '{adult_kids}'
           and ord_qty != 0
--         union all
--         select distinct cat_nm, sub_cat_nm
--         from prcs.db_srch_kwd_naver_mst
--         where brd_cd = '{brand}'
--           and adult_kids = '{adult_kids}'
--           and comp_type != '라이프스타일'
--           and cat_nm != '일반'
     ) a
order by sub_cat_nm
        """
        subcategories_query = self.get_query(
            query =query,
            brand = brand,
            adult_kids = adult_kids,
        )
            
        subcategories_redshift_data = RedshiftData(connect, subcategories_query)
        subcategories_data = subcategories_redshift_data.get_data()

        subcategories_dicts = subcategories_data.to_dict('records')

        result = {}
        for dict in subcategories_dicts:
            if dict['parent_value'] not in result.keys():
                result[dict['parent_value']] = [dict['value']]
            elif dict['parent_value'] in result.keys():
                result[dict['parent_value']].append(dict['value'])
        
        return result

    def get_seasons(self, brand, adult_kids, connect):
        query = """

select a.*, row_number() over (order by value desc) as id
from (
         select distinct trim(sesn) as value
         from prcs.db_prdt
         where brd_cd = '{brand}'
           and ord_qty != 0
         order by 1 desc
     ) a
order by id
        """
        seasons_query = self.get_query(
            query = query,
            brand = brand,
            adult_kids = adult_kids,
        )

        seasons_redshift_data = RedshiftData(connect, seasons_query)
        seasons_data = seasons_redshift_data.get_data()

        result = seasons_data.to_dict('records')

        return result

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            adult_kids = request.GET["adult_kids"]
            connect =request.connect
            
            categories = self.get_categories(brand, adult_kids, connect)
            subcategories = self.get_subcategories(brand, adult_kids, connect)
            seasons = self.get_seasons(brand, adult_kids, connect)

            return JsonResponse({"message":"success", "categories":categories, "subcategories":subcategories, 'seasons':seasons}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
