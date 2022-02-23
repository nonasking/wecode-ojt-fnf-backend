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


# 주차별 판매 현황
class WeeklySalesSummaryView(View):
    
    def get_query(self, *args, **kwargs): 
        query = """
WITH search AS (
    SELECT end_dt,
           srch_cnt AS srch_cnt_cy,
           0        AS srch_cnt_py
    FROM (
             SELECT distinct b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT distinct brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND adult_kids_nm = '{para_adult_kids}'
                     AND sub_cat_nm in {para_sub_category}
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
               AND comp_type = '자사'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
      AND b.end_dt between '{para_start_dt}' AND '{para_end_dt_this_week}'
    UNION ALL
    SELECT end_dt + 364 AS end_dt,
           0            AS srch_cnt_cy,
           srch_cnt     AS srch_cnt_py
    FROM (
             SELECT distinct b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT distinct brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND adult_kids_nm = '{para_adult_kids}'
                     AND sub_cat_nm in {para_sub_category}
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
               AND comp_type = '자사'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
      AND b.end_dt between '{para_start_dt}'-364 AND '{para_end_dt}'-364
)
, rds AS (
    SELECT end_dt
         , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_ttl
         , 0                                   AS sale_qty_kor_ttl_py
         , 0                                   AS sale_qty_kor_ttl_py2
         , stor_qty_kor
         , stock_qty                           AS stock_qty_kor
         , 0                                   AS order_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm  in {para_sub_category}
      AND a.sesn in {para_season}
      AND end_dt between '{para_start_dt}' AND '{para_end_dt_this_week}'
    UNION ALL
    SELECT end_dt + 364                        AS end_dt
         , 0                                   AS sale_qty_kor_ttl
         , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_ttl_py
         , 0                                   AS sale_qty_kor_ttl_py2
         , 0                                   AS stor_qty_kor
         , 0                                      asstock_qty
         , 0                                   AS order_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm  in {para_sub_category}
      AND a.sesn in {para_season_py}
      AND end_dt between '{para_start_dt}'-364 AND '{para_end_dt}'-364
    UNION ALL
    SELECT end_dt + 364 * 2                    AS end_dt
         , 0                                   AS sale_qty_kor_ttl
         , 0                                   AS sale_qty_kor_ttl_py
         , sale_nml_qty_cns + sale_ret_qty_cns AS sale_qty_kor_ttl_py2
         , 0                                   AS stor_qty_kor
         , 0                                      asstock_qty
         , 0                                   AS order_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm  in {para_sub_category}
      AND a.sesn in {para_season_py2}
      AND end_dt between '{para_start_dt}'-364*2 AND '{para_end_dt}'-364*2
    UNION ALL
    SELECT date(date_trunc('week', indc_dt_cnfm)) + 6 AS end_dt
         , 0
         , 0
         , 0
         , 0
         , 0
         , b.qty                                      AS order_qty
    FROM prcs.dw_ord a,
         prcs.dw_ord_scs b,
         prcs.db_prdt c
    WHERE a.prdt_cd = b.prdt_cd
      AND a.po_no = b.po_no
      AND a.brd_cd = c.brd_cd
      AND a.prdt_cd = c.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm  in {para_sub_category}
      AND a.sesn in {para_season}
      AND apv_stat = 'C'
      AND po_cntry in ('A', 'KR')
      AND po_cust_cntry != 'M'
      AND indc_dt_cnfm between '{para_start_dt}' AND '{para_end_dt}'
)
SELECT end_dt
, SUM(sale_qty_kor_ttl) AS sale_qty_kor_ttl
, SUM(sale_qty_kor_ttl_py) AS sale_qty_kor_ttl_py
, SUM(sale_qty_kor_ttl_py2) AS sale_qty_kor_ttl_py2
, SUM(stor_qty_kor) AS stor_qty_kor
, SUM(stock_qty_kor) AS stock_qty_kor
, SUM(os) AS os
, SUM(search_qty_cy) AS search_qty_cy
, SUM(search_qty_py) AS search_qty_py
FROM (
SELECT to_char(end_dt,'yy.mm.dd') AS end_dt
     , SUM(sale_qty_kor_ttl)     AS sale_qty_kor_ttl
     , SUM(sale_qty_kor_ttl_py)  AS sale_qty_kor_ttl_py
     , SUM(sale_qty_kor_ttl_py2) AS sale_qty_kor_ttl_py2
     , SUM(stor_qty_kor)         AS stor_qty_kor
     , SUM(stock_qty_kor)        AS stock_qty_kor
     , SUM(order_qty)            AS os
     , 0 AS search_qty_cy
     , 0 AS search_qty_py
FROM rds
GROUP BY end_dt
UNION ALL
SELECT to_char(end_dt, 'yy.mm.dd') AS end_dt
     , 0,0,0,0,0,0
     , SUM(srch_cnt_cy)            AS search_qty_cy
     , SUM(srch_cnt_py)            AS search_qty_py
FROM search
GROUP BY end_dt
)
GROUP BY end_dt
ORDER BY end_dt
        """.format(
            para_brand=kwargs["brand"],
            para_season=kwargs["season"],
            para_season_py=kwargs["season_py"],
            para_season_py2=kwargs["season_py2"],
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
                             "end-date", "weekly-date", "seasons", "subcategories"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kid = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["weekly-date"]
            season = request.GET.getlist("seasons",None)
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect
            
            end_date_this_week = get_last_sunday(end_date_this_week)
            end_date = end_date_this_week
            
            season_py = get_previous_season(season)
            season_py2 = get_previous_season(season_py)
            
            season = get_tuple(season)
            season_py = get_tuple(season_py)
            season_py2 = get_tuple(season_py2)
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
                season_py = season_py,
                season_py2 = season_py2                
            )
            
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()
            
            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
            """
            result = [{
                "end_dt"               : data["end_dt"],               #끝나는 시점
                "sale_qty_kor_ttl"     : data["sale_qty_kor_ttl"],     #판매량 당해
                "sale_qty_kor_ttl_py"  : data["sale_qty_kor_ttl_py"],  #판매량 전년
                "sale_qty_kor_ttl_py2" : data["sale_qty_kor_ttl_py2"], #판매량 2년전
                "os"                   : data["os"],                   #입고예정(한국)
                "stor_qty_kor"         : data["stor_qty_kor"],         #입고수량(한국)
                "stock_qty_kor"        : data["stock_qty_kor"],        #재고수량(한국)
                "search_qty_cy"        : data["search_qty_cy"],        #검색량 당해
                "search_qty_py"        : data["search_qty_py"],        #검색량 전년
                }for __, data in data.iterrows()
            ]            
            return JsonResponse({"message":"success", "data":result}, status=200)
            """
            
            fake = [{
                "end_dt": "22.01.02",
                "sale_qty_kor_ttl": 0,
                "sale_qty_kor_ttl_py": 0,
                "sale_qty_kor_ttl_py2": 0,
                "os": 0,
                "stor_qty_kor": 4500,
                "stock_qty_kor": 28453,
                "search_qty_cy": 197.0,
                "search_qty_py": 130.0
                },
                {
                "end_dt": "22.01.09",
                "sale_qty_kor_ttl": 150,
                "sale_qty_kor_ttl_py": 0,
                "sale_qty_kor_ttl_py2": 0,
                "os": 0,
                "stor_qty_kor": 0,
                "stock_qty_kor": 28167,
                "search_qty_cy": 291.0,
                "search_qty_py": 140.0
                },
                {
                "end_dt": "22.01.16",
                "sale_qty_kor_ttl": 544,
                "sale_qty_kor_ttl_py": 0,
                "sale_qty_kor_ttl_py2": 0,
                "os": 1000,
                "stor_qty_kor": 1824,
                "stock_qty_kor": 27869,
                "search_qty_cy": 426.0,
                "search_qty_py": 174.0
                },
                {
                "end_dt": "22.01.23",
                "sale_qty_kor_ttl": 437,
                "sale_qty_kor_ttl_py": 38,
                "sale_qty_kor_ttl_py2": 0,
                "os": 0,
                "stor_qty_kor": 18,
                "stock_qty_kor": 23353,
                "search_qty_cy": 462.0,
                "search_qty_py": 321.0
                },
                {
                "end_dt": "22.01.30",
                "sale_qty_kor_ttl": 385,
                "sale_qty_kor_ttl_py": 247,
                "sale_qty_kor_ttl_py2": 0,
                "os": 1900,
                "stor_qty_kor": 2153,
                "stock_qty_kor": 28618,
                "search_qty_cy": 706.0,
                "search_qty_py": 645.0
                },
                {
                "end_dt": "22.02.06",
                "sale_qty_kor_ttl": 475,
                "sale_qty_kor_ttl_py": 567,
                "sale_qty_kor_ttl_py2": 0,
                "os": 0,
                "stor_qty_kor": 0,
                "stock_qty_kor": 28000,
                "search_qty_cy": 1029.0,
                "search_qty_py": 778.0
                }]
            return JsonResponse({"message":"success", "data":fake}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)        