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


# 판매 실적 요약(시즌누계)
class SalesSummaryAccSesnView(View):
    
    def get_query(self, *args, **kwargs): 
        query = """

WITH sale AS (
    SELECT '당해'                                         term_cls
         , ac_stor_qty_kor                           AS ac_stor_qty_kor
         , sale_nml_qty_cns + sale_ret_qty_cns       AS sale_qty_kor
         , ac_sale_nml_qty_cns + ac_sale_ret_qty_cns AS ac_sale_qty_kor
         , stock_qty                                 AS stock_qty
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
    SELECT '전년'                                         term_cls
         , ac_stor_qty_kor                           AS ac_stor_qty_kor
         , sale_nml_qty_cns + sale_ret_qty_cns       AS sale_qty_kor
         , ac_sale_nml_qty_cns + ac_sale_ret_qty_cns AS ac_sale_qty_kor
         , stock_qty                                 AS stock_qty
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
     order_status AS (
         SELECT '당해'  AS term_cls
              , b.qty AS indc_qty
         FROM prcs.dw_ord a,
              prcs.dw_ord_scs b,
              prcs.db_prdt c
         WHERE a.prdt_cd = b.prdt_cd
           AND a.po_no = b.po_no
           AND a.prdt_cd = c.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season} --당해
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND apv_stat = 'C'
           AND po_cntry in ('A', 'KR')
           AND po_cust_cntry != 'M'
           AND apv_dt <= '{para_end_dt_this_week}'
         UNION ALL
         SELECT '전년'  AS term_cls
              , b.qty AS indc_qty
         FROM prcs.dw_ord a,
              prcs.dw_ord_scs b,
              prcs.db_prdt c
         WHERE a.prdt_cd = b.prdt_cd
           AND a.po_no = b.po_no
           AND a.prdt_cd = c.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season_py} --전년
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND po_cntry in ('A', 'KR')
           AND po_cust_cntry != 'M'
           AND apv_stat = 'C'
           AND apv_dt <= '{para_end_dt_this_week}' - 364
     ),
     season_end AS (
         SELECT '전년'                                         term_cls
              , ac_stor_qty_kor                           AS ac_stor_qty_kor_season_end
              , ac_sale_nml_qty_cns + ac_sale_ret_qty_cns AS ac_sale_qty_kor_season_end
              , stock_qty                                 AS stock_qty_season_end
         FROM prcs.db_scs_w a,
              prcs.db_prdt b
         WHERE a.prdt_cd = b.prdt_cd
           AND a.brd_cd = '{para_brand}'
           AND a.sesn in {para_season_py} --전년
           AND cat_nm = '{para_category}'
           AND sub_cat_nm in {para_sub_category}
           AND adult_kids_nm = '{para_adult_kids}'
           AND (
             SELECT max(last_week)
             FROM (
                      SELECT case
                                 when substring(a.sesn, 3, 1) = 'S'
                                     then date(date_trunc('week', date('20' || substring(a.sesn, 1, 2) || '0901'))) + 6
                                 when substring(a.sesn, 3, 1) = 'F'
                                     then
                                     date(date_trunc('week', date('20' || substring(a.sesn, 1, 2)::integer + 1 || '0301'))) + 6
                                 else
                                     date(date_trunc('week', date('20' || substring(a.sesn, 1, 2) || '1231'))) + 6
                                 end as last_week
                      FROM prcs.db_scs_w a,
                           prcs.db_prdt b
                      WHERE a.brd_cd = b.brd_cd
                        AND a.prdt_cd = b.prdt_cd
                        AND a.brd_cd = '{para_brand}'
                        AND cat_nm = '{para_category}'
                        AND adult_kids_nm = '{para_adult_kids}'
                        AND sub_cat_nm in {para_sub_category}
                        AND a.sesn in {para_season_py}
                  ) a
         ) between start_dt and end_dt)
SELECT term_cls,
       SUM(indc_qty)                   AS indc_qty,
       SUM(ac_stor_qty_kor)            AS ac_stor_qty_kor,
       SUM(sale_qty_kor)               AS sale_qty_kor,
       SUM(ac_sale_qty_kor)            AS ac_sale_qty_kor,
       SUM(stock_qty)                  AS stock_qty,
       SUM(ac_stor_qty_kor_season_end) AS ac_stor_qty_kor_season_end,
       SUM(ac_sale_qty_kor_season_end) AS ac_sale_qty_kor_season_end,
       SUM(stock_qty_season_end)       AS stock_qty_season_end
FROM (
         SELECT term_cls
              , indc_qty
              , 0 AS ac_stor_qty_kor
              , 0 AS sale_qty_kor
              , 0 AS ac_sale_qty_kor
              , 0 AS stock_qty
              , 0 AS ac_stor_qty_kor_season_end
              , 0 AS ac_sale_qty_kor_season_end
              , 0 AS stock_qty_season_end
         FROM order_status
         UNION ALL
         SELECT term_cls
              , 0 AS indc_qty
              , ac_stor_qty_kor
              , sale_qty_kor
              , ac_sale_qty_kor
              , stock_qty
              , 0 AS ac_stor_qty_kor_season_end
              , 0 AS ac_sale_qty_kor_season_end
              , 0 AS stock_qty_season_end
         FROM sale
         UNION ALL
         SELECT term_cls
              , 0 AS indc_qty
              , 0 AS ac_stor_qty_kor
              , 0 AS sale_qty_kor
              , 0 AS ac_sale_qty_kor
              , 0 AS stock_qty
              , ac_stor_qty_kor_season_end
              , ac_sale_qty_kor_season_end
              , stock_qty_season_end
         FROM season_end
     ) a
GROUP BY term_cls
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
            
            data['sales_rate'] = round((data['ac_sale_qty_kor']/data['ac_stor_qty_kor'])*100,0)
            data["season_end_sales_rate"] = round((data["ac_sale_qty_kor_season_end"]/data["ac_stor_qty_kor_season_end"])*100,0)
            data.fillna(0, inplace=True)
            """
            result = [{
                "term_cls"                   : item["term_cls"],                   #해당년도
                "indc_qty"                   : item["indc_qty"],                   #발주
                "ac_stor_qty_kor"            : item["ac_stor_qty_kor"],            #입고 
                "sale_qty_kor"               : item["sale_qty_kor"],               #주간판매
                "ac_sale_qty_kor"            : item["ac_sale_qty_kor"],            #누적판매
                "stock_qty"                  : item["stock_qty"],                  #재고
                "sales_rate"                 : item["sales_rate"],                 #판매율
                "ac_stor_qty_kor_season_end" : item["ac_stor_qty_kor_season_end"], #입고
                "ac_sale_qty_kor_season_end" : item["ac_sale_qty_kor_season_end"], #판매
                "stock_qty_season_end"       : item["stock_qty_season_end"],       #재고
                "season_end_sales_rate"      : item["season_end_sales_rate"]       #시즌마감 판매율
                }for __, item in data.iterrows()
            ]
            return JsonResponse({"message":"success", "data":result}, status=200)
            """
            
            fake = [{
                "term_cls": "당해",
                "indc_qty": 50000,
                "ac_stor_qty_kor": 48197,
                "sale_qty_kor": 1730,
                "ac_sale_qty_kor": 7215,
                "stock_qty": 39091,
                "sales_rate": 12.0,
                "ac_stor_qty_kor_season_end": 0,
                "ac_sale_qty_kor_season_end": 0,
                "stock_qty_season_end": 0,
                "season_end_sales_rate": 0.0
                },
                {
                "term_cls": "전년",
                "indc_qty": 31100,
                "ac_stor_qty_kor": 28880,
                "sale_qty_kor": 897,
                "ac_sale_qty_kor": 1580,
                "stock_qty": 22878,
                "sales_rate": 4.0,
                "ac_stor_qty_kor_season_end": 38853,
                "ac_sale_qty_kor_season_end": 22552,
                "stock_qty_season_end": 11847,
                "season_end_sales_rate": 53.0
                }]
            return JsonResponse({"message":"success", "data":fake}, status=200)     
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
