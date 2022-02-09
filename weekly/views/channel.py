import psycopg2
import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season

class ChannelTimeSeriesView(View):

    def __init__(self):
        self.columns_description={
            "sales_kor_cy":"위탁총판매",
            "sales_kor_retail_cy":"국내",
            "sales_dutyfwhole_cy":"면세RF도매",
            "sales_chn_cy":"중국",
            "sales_gvl_cy":"홍마대",
        }

    def get_query(self, *args, **kwargs):
        query = """
with rds AS (
    SELECT end_dt
         , sale_nml_qty_cns + sale_ret_qty_cns AS sales_kor_cy
         , sale_nml_qty_rtl + sale_ret_qty_rtl AS sales_retail_cy
         , sale_nml_qty_notax + sale_ret_qty_notax
        + sale_nml_qty_dome + sale_ret_qty_dome
        + sale_nml_qty_rf + sale_ret_qty_rf    AS sales_dutyrfwhole_cy
         , sale_nml_qty_chn + sale_ret_qty_chn AS sales_chn_cy
         , sale_nml_qty_gvl + sale_ret_qty_gvl AS sales_gvl_cy
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND sub_cat_nm IN {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND a.sesn IN {para_season}
      AND end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
)
SELECT TO_CHAR(end_dt, 'yy.mm.dd')            AS end_dt
     , SUM(sales_kor_cy)          AS sales_kor_cy
     , SUM(sales_retail_cy)       AS sales_kor_retail_cy
     , SUM(sales_dutyrfwhole_cy)  AS sales_dutyrfwhole_cy
     , SUM(sales_chn_cy)          AS sales_chn_cy
     , SUM(sales_gvl_cy)          AS sales_gvl_cy
FROM rds
GROUP BY end_dt
ORDER BY 1
           """.format(
            para_brand = kwargs["brand"],
            para_season = kwargs["season"],
            para_category = kwargs["category"],
            para_sub_category = kwargs["sub_category"],
            para_adult_kids = kwargs["adult_kids"],
            para_start_dt = kwargs["start_date"],
            para_end_dt = kwargs["end_date"],
            para_end_dt_this_week = kwargs["end_date_this_week"],
        )
        return query

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            category = request.GET["category"]
            adult_kids = request.GET["adult_kids"]
            start_date = request.GET["start_date"]
            end_date = request.GET["end_date"]
            end_date_this_week = request.GET["end_date_this_week"]
            season = request.GET.getlist("season",None)
            sub_category = request.GET.getlist("sub_category",None)
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)
            end_date = end_date_this_week

            season = get_tuple(season)
            sub_category = get_tuple(sub_category)

            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kids = adult_kids,
                start_date = start_date,
                end_date = end_date,
                end_date_this_week = end_date_this_week,
                season = season,
            )
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            result = [{
                "end_date": item["end_dt"],
                "sales_kor_cy": item["sales_kor_cy"],
                "sales_kor_retail_cy": item["sales_kor_retail_cy"],
                "sales_dutyrfwhole_cy": item["sales_dutyrfwhole_cy"],
                "sales_chn_cy": item["sales_chn_cy"],
                "sales_gvl_cy": item["sales_gvl_cy"],
                }for __, item in data.iterrows()
            ]
            return JsonResponse({"message":"success", "data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)


class ChannelTableView(View):

    def get_query(self, *args, **kwargs):
        query="""
WITH rds AS (
    SELECT cat_nm
         , sub_cat_nm
         , domain1_nm

         , sale_nml_sale_amt_cns + sale_ret_sale_amt_cns AS sales_kor_cy
         , sale_nml_sale_amt_rtl + sale_ret_sale_amt_rtl AS sales_retail_cy
         , sale_nml_sale_amt_notax + sale_ret_sale_amt_notax
        + sale_nml_sale_amt_dome + sale_ret_sale_amt_dome
        + sale_nml_sale_amt_rf + sale_ret_sale_amt_rf    AS sales_dutyrfwhole_cy
         , sale_nml_sale_amt_chn + sale_ret_sale_amt_chn AS sales_chn_cy
         , sale_nml_sale_amt_gvl + sale_ret_sale_amt_gvl AS sales_gvl_cy

         , sale_nml_qty_cns + sale_ret_qty_cns           AS qty_kor_cy
         , sale_nml_qty_rtl + sale_ret_qty_rtl           AS qty_retail_cy
         , sale_nml_qty_notax + sale_ret_qty_notax
        + sale_nml_qty_dome + sale_ret_qty_dome
        + sale_nml_qty_rf + sale_ret_qty_rf              AS qty_dutyrfwhole_cy
         , sale_nml_qty_chn + sale_ret_qty_chn           AS qty_chn_cy
         , sale_nml_qty_gvl + sale_ret_qty_gvl           AS qty_gvl_cy

         , 0                                             AS sales_kor_py
         , 0                                             AS sales_retail_py
         , 0                                             AS sales_dutyrfwhole_py
         , 0                                             AS sales_chn_py
         , 0                                             AS sales_gvl_py

         , 0                                             AS qty_kor_py
         , 0                                             AS qty_retail_py
         , 0                                             AS qty_dutyrfwhole_py
         , 0                                             AS qty_chn_py
         , 0                                             AS qty_gvl_py

         , stock_qty
    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND a.sesn IN {para_season}
      AND cat_nm = '{para_category}'
      AND sub_cat_nm IN {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND end_dt = '{para_end_dt_this_week}'
    UNION ALL
    SELECT cat_nm
         , sub_cat_nm
         , domain1_nm


         , 0                                             AS sales_kor_cy
         , 0                                             AS sales_retail_cy
         , 0                                             AS sales_dutyrfwhole_cy
         , 0                                             AS sales_chn_cy
         , 0                                             AS sales_gvl_cy
         , 0                                             AS qty_kor_cy
         , 0                                             AS qty_retail_cy
         , 0                                             AS qty_dutyrfwhole_cy
         , 0                                             AS qty_chn_cy
         , 0                                             AS qty_gvl_cy

         , sale_nml_sale_amt_cns + sale_ret_sale_amt_cns AS sales_kor_py
         , sale_nml_sale_amt_rtl + sale_ret_sale_amt_rtl AS sales_retail_py
         , sale_nml_sale_amt_notax + sale_ret_sale_amt_notax
        + sale_nml_sale_amt_dome + sale_ret_sale_amt_dome
        + sale_nml_sale_amt_rf + sale_ret_sale_amt_rf    AS sales_dutyrfwhole_py
         , sale_nml_sale_amt_chn + sale_ret_sale_amt_chn AS sales_chn_py
         , sale_nml_sale_amt_gvl + sale_ret_sale_amt_gvl AS sales_gvl_py

         , sale_nml_qty_cns + sale_ret_qty_cns           AS qty_kor_py
         , sale_nml_qty_rtl + sale_ret_qty_rtl           AS qty_retail_py
         , sale_nml_qty_notax + sale_ret_qty_notax
        + sale_nml_qty_dome + sale_ret_qty_dome
        + sale_nml_qty_rf + sale_ret_qty_rf              AS qty_dutyrfwhole_py
         , sale_nml_qty_chn + sale_ret_qty_chn           AS qty_chn_py
         , sale_nml_qty_gvl + sale_ret_qty_gvl           AS qty_gvl_py

         , 0                                             AS stock_qty

    FROM prcs.db_scs_w a,
         prcs.db_prdt b
    WHERE a.brd_cd = b.brd_cd
      AND a.prdt_cd = b.prdt_cd
      AND a.brd_cd = '{para_brand}'
      AND a.sesn IN {para_season_py}
      AND cat_nm = '{para_category}'
      AND sub_cat_nm IN {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND end_dt = '{para_end_dt_this_week}'-364
)
SELECT *
FROM (
    SELECT cls
         , sales_cy / 1000000                                                                                            AS sales_cy
         , CASE WHEN SUM(sales_cy) OVER () = 0 THEN 0 ELSE ROUND(sales_cy::NUMERIC / SUM(sales_cy) OVER () * 100, 0) END AS ratio
         , CASE WHEN qty_py = 0 THEN 0 ELSE ROUND(qty_cy::NUMERIC / qty_py * 100, 0) END                           AS growth
         , qty_cy
         , qty_py
         , sales_py
    FROM (
             SELECT cls
                  , SUM(sales_cy) AS sales_cy
                  , SUM(sales_py) AS sales_py
                  , SUM(qty_cy)   AS qty_cy
                  , SUM(qty_py)   AS qty_py
             FROM (
                      SELECT '국내'          AS cls
                           , sales_retail_cy AS sales_cy
                           , sales_retail_py AS sales_py
                           , qty_retail_cy   AS qty_cy
                           , qty_retail_py   AS qty_py
                      FROM rds
                      UNION ALL
                      SELECT '면세RF도매' AS cls
                           , sales_dutyrfwhole_cy
                           , sales_dutyrfwhole_py
                           , qty_dutyrfwhole_cy
                           , qty_dutyrfwhole_py
                      FROM rds
                  ) a
             GROUP BY cls
         ) a
     UNION ALL
     SELECT cls
         , sales_cy / 1000000                                                                                          AS sales_cy
         , CASE WHEN SUM(sales_cy) OVER () = 0 THEN 0 ELSE ROUND(sales_cy::NUMERIC / SUM(sales_cy) OVER () * 100, 0) END AS ratio
         , CASE WHEN qty_py = 0 THEN 0 ELSE ROUND(qty_cy::NUMERIC / qty_py * 100, 0) END                           AS growth
         , qty_cy
         , qty_py
         , sales_py
    FROM (
         SELECT 'Total' cls
               , SUM(sales_kor_cy) AS sales_cy
               , SUM(sales_kor_py) AS sales_py
               , SUM(qty_kor_cy)   AS qty_cy
               , SUM(qty_kor_py)   AS qty_py
         FROM rds
    )a
)
ORDER BY sales_cy DESC , cls
        """.format(
            para_brand = kwargs['brand'],
            para_season = kwargs['season'],
            para_season_py = kwargs['season_py'],
            para_category = kwargs['category'],
            para_sub_category = kwargs['sub_category'],
            para_adult_kids = kwargs['adult_kids'],
            para_end_dt_this_week = kwargs['end_date_this_week'],
        )

        return query

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            category = request.GET["category"]
            adult_kids = request.GET["adult_kids"]
            start_date = request.GET["start_date"]
            end_date = request.GET["end_date"]
            end_date_this_week = request.GET["end_date_this_week"]
            season = request.GET.getlist("season",None)
            sub_category = request.GET.getlist("sub_category",None)
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

            column = ["cls", "ratio", "qty_cy", "qty_py", "growth"]
            result = data[column].to_dict("records")
            return JsonResponse({"message":"success","data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)

