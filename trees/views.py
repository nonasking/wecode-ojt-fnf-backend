import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql

from django.http import JsonResponse
from django.views import View

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday

class CategoryTreeView(View):

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

    def get_query(self, query, *args, **kwargs):
        query = query.format(
            brand=kwargs["brand"],
            adult_kids=kwargs["adult_kids"],
        )
        return query
    
    def get_categories(self, brand, adult_kids, connect):
        categories_query = """
        
SELECT DISTINCT value
FROM (
         SELECT DISTINCT cat_nm AS value
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND adult_kids_nm = '{adult_kids}'
           AND cat_nm != 'TBA'
           AND ord_qty != 0
--         union all
--         SELECT DISTINCT cat_nm AS value
--         FROM prcs.db_srch_kwd_naver_mst
--         WHERE brd_cd = '{brand}'
--           AND adult_kids = '{adult_kids}'
--           AND comp_type != '라이프스타일'
--           AND cat_nm != '일반'
     ) a
ORDER BY 1        

        """
        query = self.get_query(
            query = categories_query,
            brand = brand,
            adult_kids = adult_kids,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        categories_list = data.values.tolist()

        result = []
        for category in categories_list:
            result += category
                
        return result
    
    def get_subcategories(self, brand, adult_kids, connect):
        subcategories_query = """

SELECT DISTINCT sub_cat_nm AS value, cat_nm AS parent_value
FROM (
         SELECT DISTINCT cat_nm, sub_cat_nm
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND adult_kids_nm = '{adult_kids}'
           AND ord_qty != 0
--         union all
--         SELECT DISTINCT cat_nm, sub_cat_nm
--         FROM prcs.db_srch_kwd_naver_mst
--         WHERE brd_cd = '{brand}'
--           AND adult_kids = '{adult_kids}'
--           AND comp_type != '라이프스타일'
--           AND cat_nm != '일반'
     ) a
ORDER BY sub_cat_nm
        """
        query = self.get_query(
            query = subcategories_query,
            brand = brand,
            adult_kids = adult_kids,
        )
            
        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        subcategories_dicts = data.to_dict('records')

        result = {}
        for dict in subcategories_dicts:
            if dict['parent_value'] not in result.keys():
                result[dict['parent_value']] = [dict['value']]
            elif dict['parent_value'] in result.keys():
                result[dict['parent_value']].append(dict['value'])
        
        return result

    def get_seasons(self, brand, adult_kids, connect):
        seasons_query = """

SELECT a.*, row_number() OVER (order by value desc) AS id
FROM (
         SELECT DISTINCT trim(sesn) AS value
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND ord_qty != 0
         ORDER BY 1 desc
     ) a
ORDER BY id
        """
        query = self.get_query(
            query = seasons_query,
            brand = brand,
            adult_kids = adult_kids,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()
        
        result = data['value'].tolist()

        return result

