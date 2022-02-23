import psycopg2
import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season
from utils.check_item import check_keys_in_dictionary


class SubcategoryTimeSeriesView(View):

    def get_query(self, *args, **kwargs):
        query = """
SELECT TO_CHAR(end_dt,'yy.mm.dd') AS end_date
     , sub_cat_nm
     , SUM(sale_nml_qty_cns + sale_ret_qty_cns) as qty
FROM prcs.db_scs_w a,
     prcs.db_prdt b
WHERE a.brd_cd = b.brd_cd
  AND a.prdt_cd = b.prdt_cd
  AND a.brd_cd = '{para_brand}'
  AND a.sesn IN {para_season}
  AND cat_nm = '{para_category}'
  AND sub_cat_nm IN {para_sub_category}
  AND adult_kids_nm = '{para_adult_kids}'
  AND end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
GROUP BY end_date
       , sub_cat_nm
ORDER BY end_date, sub_cat_nm
        """.format(
            para_brand = kwargs["brand"],
            para_season = kwargs["season"],
            para_category = kwargs["category"],
            para_sub_category = kwargs["sub_category"],
            para_adult_kids = kwargs["adult_kids"],
            para_start_dt = kwargs["start_date"],
            para_end_dt_this_week = kwargs["end_date_this_week"],
        )
        return query

    #@connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            '''
            required_keys = ["brand", "categories", "adult-kids", "start-date",
                             "weekly-date", "seasons", "subcategories"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date_this_week = request.GET["weekly-date"]
            season = request.GET.getlist("seasons", None)
            sub_category = request.GET.getlist("subcategories", None)
            connect = request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            season = get_tuple(season)
            sub_category = get_tuple(sub_category)

            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kids = adult_kids,
                start_date = start_date,
                end_date_this_week = end_date_this_week,
                season = season,
            )
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            pivot_data = data\
                        .pivot(index="end_date", columns="sub_cat_nm", values="qty")\
                        .fillna(0)

            pivot_data.columns = pivot_data.columns.values
            pivot_data.reset_index(inplace=True)
            columns = pivot_data.columns.tolist()

            result = [{
                column:item[column] for column in columns
            }for __, item in pivot_data.iterrows()]
            '''
            result = [
                {
                     "end_date": "22.01.02",
                     "기타아우터": 0,
                     "바람막이": 0,
                     "베이스볼점퍼": 191
                },
                {
                     "end_date": "22.01.09",
                     "기타아우터": 0,
                     "바람막이": 166,
                     "베이스볼점퍼": 910
                },
                {
                     "end_date": "22.01.16",
                     "기타아우터": 0,
                     "바람막이": 744,
                     "베이스볼점퍼": 511
                },
                {
                    "end_date": "22.01.23",
                    "기타아우터": 513,
                    "바람막이": 537,
                    "베이스볼점퍼": 653
                },
                {
                    "end_date": "22.01.30",
                    "기타아우터": 1215,
                    "바람막이": 485,
                    "베이스볼점퍼": 558
                },
                {
                    "end_date": "22.02.06",
                    "기타아우터": 643,
                    "바람막이": 575,
                    "베이스볼점퍼": 812
                }
            ]
            return JsonResponse({"message":"success","data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)


class SubcategoryTableView(View):
    
    def get_query(self, *args, **kwargs):
        query = """
WITH main AS (
    SELECT sub_cat_nm
         , parent_prdt_kind_nm
         , SUM(week_sale_amt_cy)                  AS week_sale_amt_cy
         , SUM(week_sale_qty_cy)                  AS week_sale_qty_cy
         , SUM(week_sale_amt_py)                  AS week_sale_amt_py
         , SUM(week_sale_qty_py)                  AS week_sale_qty_py
         , SUM(ac_stor_qty_cy)                    AS ac_stor_qty_cy
         , SUM(ac_sale_qty_cy)                    AS ac_sale_qty_cy
         , SUM(stock_qty)                         AS stock_qty
         , ROUND(SUM(sale_qty_4wks)::NUMERIC / 4) AS sale_qty_4wks
    FROM (
             SELECT cat_nm
                  , sub_cat_nm
                  , domain1_nm
                  , parent_prdt_kind_nm
                  , style_cd
                  , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN sale_nml_sale_amt_cns + sale_ret_sale_amt_cns ELSE 0 END                    AS week_sale_amt_cy
                  , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN sale_nml_qty_cns + sale_ret_qty_cns ELSE 0 END                              AS week_sale_qty_cy
                  , 0                                                                                                                             AS week_sale_amt_py
                  , 0                                                                                                                             AS week_sale_qty_py
                  , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN ac_stor_qty_kor ELSE 0 END                                                  AS ac_stor_qty_cy
                  , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN ac_sale_nml_qty_cns + ac_sale_ret_qty_cns ELSE 0 END                        AS ac_sale_qty_cy
                  , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN stock_qty ELSE 0 END                                                        AS stock_qty
                  , CASE WHEN end_dt BETWEEN '{para_end_dt_this_week}' - 3 * 7 AND '{para_end_dt_this_week}' THEN sale_nml_qty_cns + sale_ret_qty_cns ELSE 0 END AS sale_qty_4wks
             FROM prcs.db_scs_w a,
                  prcs.db_prdt b
             WHERE a.brd_cd = b.brd_cd
               AND a.prdt_cd = b.prdt_cd
               AND a.brd_cd = '{para_brand}'
               AND a.sesn IN {para_season}
               AND cat_nm = '{para_category}'
               AND sub_cat_nm IN {para_sub_category}
               AND adult_kids_nm = '{para_adult_kids}'
               AND end_dt BETWEEN '{para_end_dt_this_week}' - 3 * 7 AND '{para_end_dt_this_week}'
             UNION ALL
             SELECT cat_nm
                  , sub_cat_nm
                  , domain1_nm
                  , parent_prdt_kind_nm
                  , style_cd
                  , 0                                             AS week_sale_amt_cy
                  , 0                                             AS week_sale_qty_cy
                  , sale_nml_sale_amt_cns + sale_ret_sale_amt_cns AS week_sale_amt_py
                  , sale_nml_qty_cns + sale_ret_qty_cns           AS week_sale_qty_py
                  , 0
                  , 0
                  , 0
                  , 0
             FROM prcs.db_scs_w a,
                  prcs.db_prdt b
             WHERE a.brd_cd = b.brd_cd
               AND a.prdt_cd = b.prdt_cd
               AND a.brd_cd = '{para_brand}'
               AND a.sesn IN {para_season_py}
               AND cat_nm = '{para_category}'
               AND sub_cat_nm IN {para_sub_category}
               AND adult_kids_nm = '{para_adult_kids}'
               AND end_dt = '{para_end_dt_this_week}' - 364
         ) a
    GROUP BY sub_cat_nm, parent_prdt_kind_nm
)
SELECT *
FROM (
         SELECT sub_cat_nm,
                stock_qty,
                CASE WHEN SUM(week_sale_amt_cy) OVER () = 0 THEN 0 ELSE ROUND(week_sale_amt_cy::NUMERIC / SUM(week_sale_amt_cy) OVER () * 100) END AS week_ratio,
                CASE WHEN week_sale_amt_py = 0 THEN 0 ELSE ROUND(week_sale_amt_cy::NUMERIC / week_sale_amt_py * 100) END                           AS week_growth,
                CASE
                    WHEN parent_prdt_kind_nm = '의류' AND week_sale_qty_cy != 0 THEN ROUND(stock_qty::NUMERIC / week_sale_qty_cy::NUMERIC)::DECIMAL
                    WHEN parent_prdt_kind_nm = '의류' AND week_sale_qty_cy = 0 THEN stock_qty::NUMERIC
                    WHEN parent_prdt_kind_nm = 'ACC' AND sale_qty_4wks != 0 THEN ROUND(stock_qty::NUMERIC / sale_qty_4wks::NUMERIC)::DECIMAL
                    WHEN parent_prdt_kind_nm = 'ACC' AND sale_qty_4wks = 0 THEN stock_qty::NUMERIC
                    ELSE stock_qty END ::DECIMAL                                                                                                   AS woi,
                CASE
                    WHEN parent_prdt_kind_nm = '의류' AND ac_stor_qty_cy != 0 THEN ROUND(ac_sale_qty_cy::NUMERIC / ac_stor_qty_cy::NUMERIC * 100)::DECIMAL
                    ELSE 0 END ::decimal                                                                                                           AS sale_rate,
                sale_qty_4wks                                                                                                                      AS sale_qty_4wks,
                week_sale_qty_cy,
                week_sale_amt_cy
         FROM main
         UNION ALL
         SELECT 'Total'                                                                                                                            AS sub_cat_nm,
                stock_qty,
                CASE WHEN SUM(week_sale_amt_cy) OVER () = 0 THEN 0 ELSE ROUND(week_sale_amt_cy::NUMERIC / SUM(week_sale_amt_cy) OVER () * 100) END AS week_ratio,
                CASE WHEN week_sale_amt_py = 0 THEN 0 ELSE ROUND(week_sale_amt_cy::NUMERIC / week_sale_amt_py * 100) END                           AS week_growth,
                CASE
                    WHEN parent_prdt_kind_nm = '의류' AND week_sale_qty_cy != 0 THEN ROUND(stock_qty::NUMERIC / week_sale_qty_cy::NUMERIC)::DECIMAL
                    WHEN parent_prdt_kind_nm = '의류' AND week_sale_qty_cy = 0 THEN stock_qty::NUMERIC
                    WHEN parent_prdt_kind_nm = 'ACC' AND sale_qty_4wks != 0 THEN ROUND(stock_qty::NUMERIC / sale_qty_4wks::NUMERIC)::DECIMAL
                    WHEN parent_prdt_kind_nm = 'ACC' AND sale_qty_4wks = 0 THEN stock_qty::NUMERIC
                    ELSE stock_qty END ::DECIMAL                                                                                                   AS woi,
                CASE
                    WHEN parent_prdt_kind_nm = '의류' AND ac_stor_qty_cy != 0 THEN ROUND(ac_sale_qty_cy::NUMERIC / ac_stor_qty_cy::NUMERIC * 100)::DECIMAL
                    ELSE 0 END ::DECIMAL                                                                                                           AS sale_rate,
                sale_qty_4wks                                                                                                                      AS sale_qty_4wks,
                week_sale_qty_cy,
                week_sale_amt_cy
         FROM (
                  SELECT parent_prdt_kind_nm,
                         SUM(week_sale_amt_cy) AS week_sale_amt_cy,
                         SUM(week_sale_qty_cy) AS week_sale_qty_cy,
                         SUM(week_sale_amt_py) AS week_sale_amt_py,
                         SUM(week_sale_qty_py) AS week_sale_qty_py,
                         SUM(ac_stor_qty_cy)   AS ac_stor_qty_cy,
                         SUM(ac_sale_qty_cy)   AS ac_sale_qty_cy,
                         SUM(stock_qty)        AS stock_qty,
                         SUM(sale_qty_4wks)    AS sale_qty_4wks
                  FROM main
                  GROUP BY parent_prdt_kind_nm) a
     ) a
ORDER BY week_sale_amt_cy desc, sub_cat_nm asc
        """.format(
           para_brand = kwargs['brand'],
            para_season = kwargs['season'],
            para_season_py = kwargs['season_py'],
            para_category = kwargs['category'],
            para_sub_category = kwargs['sub_category'],
            para_adult_kids = kwargs['adult_kids'],
            para_end_dt_this_week=kwargs['end_date_this_week'],
        )

        return query

    #@connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            '''
            required_keys = ["brand", "categories", "adult-kids", "start-date",
                             "end-date", "weekly-date", "seasons", "subcategories"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["weekly-date"]
            season = request.GET.getlist("seasons",None)
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)
            season_py = get_previous_season(season)
            
            season = get_tuple(season)
            season_py = get_tuple(season_py)
            sub_category = get_tuple(sub_category)

            query = self.get_query(
                 brand = brand,
                 category = category,
                 sub_category = sub_category,
                 adult_kids = adult_kids,
                 season = season,
                 season_py = season_py,
                 end_date_this_week = end_date_this_week,
            )

            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            columns = ["sub_cat_nm", "week_sale_qty_cy", "stock_qty", "woi", 
                       "week_ratio", "week_growth", "sale_rate",]
            result = [{
                column:item[column] for column in columns
                }for __, item in data.iterrows()]
            '''
            result = [
               {
                    "sub_cat_nm": "Total",
                    "week_sale_qty_cy": 2030,
                    "stock_qty": 45091,
                    "woi": 22.0,
                    "week_ratio": 100.0,
                    "week_growth": 260.0,
                    "sale_rate": 16.0
                },
                {
                    "sub_cat_nm": "기타아우터",
                    "week_sale_qty_cy": 643,
                    "stock_qty": 1718,
                    "woi": 3.0,
                    "week_ratio": 42.0,
                    "week_growth": 597.0,
                    "sale_rate": 58.0
                },
                {
                    "sub_cat_nm": "베이스볼점퍼",
                    "week_sale_qty_cy": 812,
                    "stock_qty": 11373,
                    "woi": 14.0,
                    "week_ratio": 37.0,
                    "week_growth": 328.0,
                    "sale_rate": 24.0
                },
                {
                    "sub_cat_nm": "바람막이",
                    "week_sale_qty_cy": 575,
                    "stock_qty": 32000,
                    "woi": 56.0,
                    "week_ratio": 21.0,
                    "week_growth": 104.0,
                    "sale_rate": 7.0
                }
            ]
            return JsonResponse({"message":"success", "data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
