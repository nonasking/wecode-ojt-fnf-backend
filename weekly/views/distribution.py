import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season
from utils.check_item import check_keys_in_dictionary


class DistributionTimeSeriesView(View):

    def get_query(self, *args, **kwargs):
        query = """
WITH rds AS (
    SELECT anal_dist_type_nm
         , end_dt
         , a.shop_id
         , shop_nm_short
         , SUM(sale_nml_qty + sale_ret_qty) AS sales_cy
         , 0                                AS sales_py
    FROM prcs.db_sh_s_w a,
         prcs.db_shop b,
         prcs.db_prdt c
    WHERE a.brd_cd = b.brd_cd
      AND a.shop_id = b.shop_id
      AND a.prdt_cd = c.prdt_cd
      AND mng_type = 'A'
      AND anal_cntry = 'KO'
      AND a.brd_cd = '{para_brand}'
      AND cat_nm = '{para_category}'
      AND sub_cat_nm IN {para_sub_category}
      AND adult_kids_nm = '{para_adult_kids}'
      AND a.sesn IN {para_season}
      AND end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
    GROUP BY anal_dist_type_nm
           , a.shop_id
           , shop_nm
           , shop_nm_short
           , end_dt
    HAVING SUM(sale_nml_sale_amt + sale_ret_sale_amt) != 0

)
SELECT TO_CHAR(end_dt, 'yy.mm.dd') AS end_date
     , anal_dist_type_nm
     , SUM(sales_cy) AS sales_cy
FROM rds
GROUP BY end_date,anal_dist_type_nm
ORDER BY end_date
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

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ["brand", "categories", "adult-kids", "start-date",
                             "end-date-this-week", "seasons", "subcategories"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date_this_week = request.GET["end-date-this-week"]
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
                        .pivot(index="end_date", columns="anal_dist_type_nm", values="sales_cy")\
                        .fillna(0)
            pivot_data.columns = pivot_data.columns.values
            pivot_data.reset_index(inplace=True)
            columns = pivot_data.columns.tolist()

            result = [{
                column:item[column] for column in columns
            }for __, item in pivot_data.iterrows()]

            return JsonResponse({"message":"success","data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)


class DistributionTableView(View):

    def get_query(self, *args, **kwargs):
        query="""
WITH rds AS (
    SELECT anal_dist_type_nm
         , sale_nml_sale_amt + sale_ret_sale_amt AS sales_cy
         , sale_nml_qty + sale_ret_qty           AS qty_cy
         , 0                                     AS sales_py
         , 0                                     AS qty_py
    FROM prcs.db_sh_scs_w_rpt a,
         prcs.db_shop b,
         prcs.db_prdt c
    WHERE a.brd_cd = b.brd_cd
      AND a.shop_id = b.shop_id
      AND a.prdt_cd = c.prdt_cd
      AND mng_type = 'A'
      AND anal_cntry = 'KO'
      AND a.brd_cd = '{para_brand}'
      AND a.sesn IN {para_season}
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm IN {para_sub_category}
      AND end_dt = '{para_end_dt_this_week}'
    UNION ALL
    SELECT anal_dist_type_nm
         , 0                                     AS sales_cy
         , 0                                     AS qty_cy
         , sale_nml_sale_amt + sale_ret_sale_amt AS sales_py
         , sale_nml_qty + sale_ret_qty           AS qty_py
    FROM prcs.db_sh_scs_w_rpt a,
         prcs.db_shop b,
         prcs.db_prdt c
    WHERE a.brd_cd = b.brd_cd
      AND a.shop_id = b.shop_id
      AND a.prdt_cd = c.prdt_cd
      AND mng_type = 'A'
      AND anal_cntry = 'KO'
      AND a.brd_cd = '{para_brand}'
      AND a.sesn IN {para_season_py}
      AND cat_nm = '{para_category}'
      AND adult_kids_nm = '{para_adult_kids}'
      AND sub_cat_nm IN {para_sub_category}
      AND end_dt = '{para_end_dt_this_week}'-364
)
SELECT *
FROM (
    SELECT anal_dist_type_nm AS cls
         , CASE WHEN SUM(sales_cy) OVER () = 0 THEN 0 ELSE ROUND(sales_cy::NUMERIC / SUM(sales_cy) OVER () * 100) END AS ratio
         , CASE WHEN qty_py = 0 THEN 0 ELSE ROUND(qty_cy::NUMERIC / qty_py * 100) END                                 AS growth
         , qty_cy,qty_py, sales_cy
    FROM (
             SELECT anal_dist_type_nm
                  , SUM(sales_cy) AS sales_cy
                  , SUM(qty_cy)   AS qty_cy
                  , SUM(sales_py) AS sales_py
                  , SUM(qty_py)   AS qty_py
             FROM rds
             GROUP BY anal_dist_type_nm
         ) a
    UNION ALL
    SELECT anal_dist_type_nm AS cls
         , CASE WHEN SUM(sales_cy) OVER () = 0 THEN 0 ELSE ROUND(sales_cy::NUMERIC / SUM(sales_cy) OVER () * 100) END AS ratio
         , CASE WHEN qty_py = 0 THEN 0 ELSE ROUND(qty_cy::NUMERIC / qty_py * 100) END                                 AS growth
         , qty_cy,qty_py, sales_cy
    FROM (
             SELECT 'Total' AS anal_dist_type_nm
                  , SUM(sales_cy) AS sales_cy
                  , SUM(qty_cy)   AS qty_cy
                  , SUM(sales_py) AS sales_py
                  , SUM(qty_py)   AS qty_py
             FROM rds
         ) a
)a2
ORDER BY sales_cy DESC
        """.format(
            para_brand=kwargs['brand'],
            para_season=kwargs['season'],
            para_season_py=kwargs['season_py'],
            para_category=kwargs['category'],
            para_sub_category=kwargs['sub_category'],
            para_adult_kids=kwargs['adult_kids'],
            para_end_dt_this_week=kwargs['end_date_this_week'],
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
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["end-date-this-week"]
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
            
            columns = ["cls", "ratio", "qty_cy", "qty_py", "growth"]
            result = data[columns].to_dict("records")

            return JsonResponse({"message":"success","data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)

