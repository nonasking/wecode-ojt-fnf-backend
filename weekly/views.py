import psycopg2
import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday

class TimeSeriesView(View):

    def __init__(self):
        self.columns_description={
            "sales_kor_cy":"국내총판매",
            "sales_kor_retail_cy":"국내",
            "sales_dutyfwhole_cy":"면세RF도매",
            "sales_chn_cy":"중국",
            "sales_chn_cy":"홍마대",
        }

    def get_query(self, *args, **kwargs):
        query = """
with rds as (
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
            para_adult_kids = kwargs["adult_kid"],
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
            adult_kid = request.GET["adult_kid"]
            start_date = request.GET["start_date"]
            end_date = request.GET["end_date"]
            end_date_this_week = request.GET["end_date_this_week"]
            season = request.GET.getlist("season",None)
            sub_category = request.GET.getlist("sub_category",None)
            connect =request.connect

            end_date = get_last_sunday(end_date)
            end_date_this_week = get_last_sunday(end_date_this_week)

            season = get_tuple(season)
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
            )
        
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            columns = data.columns.values.tolist()
            result = [{
                column : data[column].tolist()
                }for column in columns
            ]
            result.append(self.columns_description)

            return JsonResponse({"message":"success", "data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
