import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season
from utils.check_item import check_keys_in_dictionary


# 판매 실적 요약(주간)
class SalesSummaryAccView(View):
    
    def get_query(self, *args, **kwargs): 
        query = """
WITH int_stock AS (
    SELECT '당해'         term_cls
         , stock_qty AS int_stock_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND a.sesn in {para_season} --당해
      AND cat_nm = '{para_category}'
      AND sub_cat_nm in {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND end_dt = '{para_end_dt_this_week}' - 7
    UNION ALL
    SELECT '전년'         term_cls
         , stock_qty as int_stock_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND a.sesn in {para_season_py} --전년
      AND cat_nm = '{para_category}'
      AND sub_cat_nm in {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND end_dt = '{para_end_dt_this_week}' - 364 - 7
),
     term_sales AS (
         SELECT '당해'       term_cls
              , stor_qty_kor                        AS stor_qty_kor_term
              , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_term
              , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_w
              , stock_qty                           AS stock_kor

         FROM prcs.db_scs_w a,
              prcs.db_prdt b
         WHERE a.prdt_cd = b.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season} --당해
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND end_dt = '{para_end_dt_this_week}'
         UNION ALL
         SELECT '전년'       term_cls
              , stor_qty_kor                        AS stor_qty_kor_term
              , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_term
              , sale_nml_qty_cns + sale_ret_qty_cns AS  sale_qty_w
              , stock_qty                           AS stock_kor
         FROM prcs.db_scs_w a,
              prcs.db_prdt b
         WHERE a.prdt_cd = b.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season_py} --전년
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND end_dt = '{para_end_dt_this_week}' - 364
     ),

     sale_4wk AS (
         SELECT '당해'       term_cls
              , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_4wk
         FROM prcs.db_scs_w a,
              prcs.db_prdt b
         WHERE a.prdt_cd = b.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season} --당해
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND end_dt between '{para_end_dt_this_week}' - 7 * 3 AND '{para_end_dt_this_week}'
         UNION ALL
         SELECT '전년'       term_cls
              , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_4wk
         FROM prcs.db_scs_w a,
              prcs.db_prdt b
         WHERE a.prdt_cd = b.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season_py} --전년
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND end_dt between '{para_end_dt_this_week}' - 364 - 7 * 3 AND '{para_end_dt_this_week}' - 364
     )
SELECT term_cls
     , int_stock_qty
     , stor_qty_kor_term
     , sale_qty_kor_term
     , sale_qty_w
     , avg_4wk_sale_qty
     , stock_kor
     , case when sale_qty_w = 0 then 0 else ROUND(stock_kor::numeric / sale_qty_w) end           AS woi
     , case when avg_4wk_sale_qty=0 then 0 else ROUND(stock_kor::numeric / avg_4wk_sale_qty) end AS woi_4wks
FROM (
         SELECT term_cls
              , SUM(int_stock_qty)                  int_stock_qty
              , SUM(stor_qty_kor_term)              stor_qty_kor_term
              , SUM(sale_qty_kor_term)              sale_qty_kor_term
              , SUM(sale_qty_w)                     sale_qty_w
              , SUM(stock_kor)                      stock_kor
              , ROUND(SUM(sale_qty_kor_4wk) / 4) AS avg_4wk_sale_qty
         FROM (
                  SELECT term_cls
                       , 0 AS int_stock_qty
                       , 0 AS stor_qty_kor_term
                       , 0 AS sale_qty_kor_term
                       , 0 AS sale_qty_w
                       , 0 AS stock_kor
                       , sale_qty_kor_4wk
                  FROM sale_4wk
                  UNION ALL
                  SELECT term_cls
                       , 0 AS int_stock_qty
                       , stor_qty_kor_term
                       , sale_qty_kor_term
                       , sale_qty_w
                       , stock_kor
                       , 0 AS sale_qty_kor_4wk
                  FROM term_sales
                  UNION ALL
                  SELECT term_cls
                       , int_stock_qty
                       , 0 AS stor_qty_kor_term
                       , 0 AS sale_qty_kor_term
                       , 0 AS sale_qty_w
                       , 0 AS stock_kor
                       , 0 AS sale_qty_kor_4wk
                  FROM int_stock
              ) a
         GROUP BY term_cls
     ) a
ORDER BY term_cls

        """.format(
            para_brand=kwargs["brand"],
            para_season=kwargs["season"],
            para_season_py=kwargs["season_py"],
            para_category=kwargs["category"],
            para_sub_category=kwargs["sub_category"],
            para_adult_kids=kwargs["adult_kid"],
            para_start_dt=kwargs["start_date"],
            para_end_dt=kwargs["end_date"],
            para_end_dt_this_week=kwargs["end_date_this_week"],
        )
        return query
    
    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ["brand", "categories", "adult-kids", "start-date",
                             "end-date", "end-date-this-week", "seasons", "subcategories"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kid = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["end-date-this-week"]
            season = request.GET.getlist("seasons",None)
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect
            
            end_date_this_week = get_last_sunday(end_date_this_week)
            end_date = end_date_this_week

            season_py = get_previous_season(season)
            season = get_tuple(season)
            season_py = get_tuple(season_py)
            sub_category = get_tuple(sub_category)
            
            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kid = adult_kid,
                start_date = start_date,
                end_date = end_date,
                end_date_this_week = end_date_this_week,
                season = season,
                season_py = season_py                
            )
            
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()
            
            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
            
            result = [{
                "term_cls": item["term_cls"],                       #해당년도
                "int_stock_qty": item["int_stock_qty"],             #기초재고
                "stor_qty_kor_term": item["stor_qty_kor_term"],     #주간입고 
                "sale_qty_w	": item["sale_qty_w"],                  #주간판매
                "avg_4wk_sale_qty": item["avg_4wk_sale_qty"],       #4주평균
                "stock_kor": item["stock_kor"],                     #기말재고
                "woi_4wks": item["woi_4wks"],                       #재고주수:4주평균     
                }for __, item in data.iterrows()
            ]
            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
