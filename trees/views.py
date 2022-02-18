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
from utils.check_item import check_keys_in_dictionary

class CategoryTreeView(View):

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ["brand", "adult-kids"]
            check_keys_in_dictionary(request.GET, required_keys)
            
            brand = request.GET["brand"]
            adult_kids = request.GET["adult-kids"]
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
--         UNION ALL
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

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data = data.to_dict('list')
        
        result = data['value']
                
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
--         UNION ALL
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
        
        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data = data.groupby('parent_value').agg({'value':lambda x: list(x)})
        data = data.to_dict()

        result = data['value']

        return result

    def get_seasons(self, brand, adult_kids, connect):
        seasons_query = """

SELECT a.*, row_number() OVER (order by value desc) AS id
FROM (
         SELECT DISTINCT trim(sesn) AS value
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND ord_qty != 0
         ORDER BY 1 DESC
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

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
        
        result = data['value'].tolist()

        return result

class StyleRankingTreeView(View):
    
    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ["brand"]
            check_keys_in_dictionary(request.GET, required_keys)
            
            brand = request.GET["brand"]
            connect =request.connect
            
            categories = self.get_categories(brand, connect)
            domains = self.get_domains(brand, connect)
            seasons = self.get_seasons(brand, connect)
            items = self.get_items(brand,connect)
            adult_kids = self.get_adult_kids(brand,connect)

            return JsonResponse({"message":"success", "categories":categories, 'domains':domains, 'seasons':seasons, 'items':items, 'adult_kids':adult_kids}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_query(self, query, *args, **kwargs):
        query = query.format(
            brand=kwargs["brand"],
        )
        return query
    
    def get_categories(self, brand, connect):
        categories_query = """

select distinct cat_nm as category, sub_cat_nm as sub_category
from prcs.db_prdt
where brd_cd = '{brand}'
order by 1,2

        """
        query = self.get_query(
            query = categories_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()
        data = data.groupby('category').agg({'sub_category':lambda x: list(x)})
        data = data.to_dict()
        
        result = data['sub_category']
        
        result = [{'value':'p'+key, 'label':key, 'children':result[key]} for key in result]

        for d in result:
            d['children'] = [{'value':v, 'label':v} for v in d['children']]

        return result
    
    def get_domains(self, brand, connect):
        domains_query = """

select case when domain1_nm is null then 'TBA' else domain1_nm end as domain
from (
         select distinct domain1_nm
         from prcs.db_prdt
         where brd_cd = '{brand}'
     ) a
order by 1

            """
        query = self.get_query(
            query = domains_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        data['value'] = data['domain']
        data.columns = ['value', 'label']        
        
        result = data.to_dict('records')
        
        return result

    def get_seasons(self, brand, connect):
        seasons_query = """

select substring(sesn, 1, 2) as yy, sesn as season, sesn || '_' || sesn_sub_nm as subseason_nm
from (
         select distinct sesn, sesn_sub_nm
         from prcs.db_prdt
         where brd_cd = '{brand}'
     ) a
order by 1 desc, 2 desc, 3
            """
        query = self.get_query(
            query = seasons_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        data = data.groupby(['yy', 'season'])
        data = data.first()
        data.columns = data.columns.values
        data.reset_index(inplace=True)

        seasons_dict = {}
        for item in data.itertuples():
            if item[1] not in seasons_dict.keys():
                seasons_dict[item[1]] = [{'season': item[2], 'subseason': item[3]}]
            elif item[1] in seasons_dict.keys():
                seasons_dict[item[1]] += [{'season': item[2], 'subseason': item[3]}]

        
        result = [{
            'value': 'pp'+key,
            'label': key,
            'children': [{
                'value': 'p'+d['season'],
                'label': d['season'],
                'children': [{
                    'value': d['subseason'],
                    'label': d['subseason']
                }]
            } for d in value]
        }for key,value in seasons_dict.items()]

        return result

    def get_items(self, brand, connect):
        items_query = """

    select distinct parent_prdt_kind_nm, prdt_kind_nm, item
from prcs.db_prdt
where brd_cd = '{brand}'

        """
        query = self.get_query(
            query = items_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()
        
        items_dict = {}
        for item in data.itertuples():
            if item[1] not in items_dict.keys():
                items_dict[item[1]] = {item[2]: [item[3]]}
            elif item[1] in items_dict.keys():
                if item[2] not in items_dict[item[1]].keys():
                    items_dict[item[1]][item[2]] = [item[3]]
                elif item[2] in items_dict[item[1]].keys():
                    items_dict[item[1]][item[2]] += [item[3]]
        
        result = [{
            'value': 'pp'+key,
            'label': key,
            'children': [{
                'value': 'p'+k,
                'label': k,
                'children': [{
                    'value': vv,
                    'label': vv
                } for vv in v]
            } for k,v in value.items()]
        }for key,value in items_dict.items()]
        
        return result

    def get_adult_kids(self, brand, connect):
        adult_kids_query = """

select distinct adult_kids_nm as adult_kids_nm
from prcs.db_prdt
where brd_cd = '{brand}'
order by 1

        """
        query = self.get_query(
            query = adult_kids_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        data['value'] = data['adult_kids_nm']
        data.columns = ['value', 'label'] 

        result = data.to_dict('records')
        
        return result