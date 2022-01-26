import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.connect_redshift import connect_redshift

class TimeSeriesView(View):

    def get_query(brand, season, category, sub_category, adult_kid,
                  start_date, end_date, end_date_this_week):

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
      AND sub_cat_nm IN ({para_sub_category})
      AND adult_kids_nm = '{para_adult_kids}'
      AND a.sesn IN ({para_season})
      AND end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
)
SELECT TO_CHAR(end_dt, 'yy.mm.dd')            AS end_dt
     , SUM(sales_kor_cy)          AS sales_kor_cy
     , SUM(sales_retail_cy)       AS sales_retail_cy
     , SUM(sales_dutyrfwhole_cy)  AS sales_dutyrfwhole_cy
     , SUM(sales_chn_cy)          AS sales_chn_cy
     , SUM(sales_gvl_cy)          AS sales_gvl_cy
FROM rds
GROUP BY end_dt
ORDER BY 1

           """.format(
            para_brand=brand,
            para_season=season,
            para_category=category,
            para_sub_category=sub_category,
            para_adult_kids=adult_kid,
            para_start_dt=start_date,
            para_end_dt=end_date,
            para_end_dt_this_week=end_date_this_week,
        )

        return query
    
    @connect_redshift
    def get(self, request, *args, **kwargs):
        brand = request.GET["brand"]
        category = request.GET["category"]
        adult_kid = request.GET["adult_kid"]
        start_date = request.GET["start_date"]
        end_date = request.GET["end_date"]
        season = request.GET.getlist["season"]
        sub_category = request.GET.getlist["sub_category"]

        query = get_query(
            brand = brand,
            category = category,
            sub_category = sub_category,
            adult_kid = adult_kid,
            start_date = start_date,
            end_date = end_date,
            season = season,
        )

        

        return JsonResponse({'message':'SUCCESS'}, status=200)
