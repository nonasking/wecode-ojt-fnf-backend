import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season


class StyleSaleView(View):

    def get_query(self, *args, **kwargs):
        query = """
WITH main AS (SELECT style_cd,
                     MAX(prdt_nm)             AS prdt_nm,
                     MAX(parent_prdt_kind_nm) AS parent_prdt_kind_nm,
                     SUM(week_sale_amt_cy)    AS week_sale_amt_cy,
                     SUM(week_sale_qty_cy)    AS week_sale_qty_cy,
                     SUM(ac_stor_qty_cy)      AS ac_stor_qty_cy,
                     SUM(ac_sale_qty_cy)      AS ac_sale_qty_cy,
                     SUM(stock_qty)           AS stock_qty,
                     SUM(sale_qty_4wks)       AS sale_qty_4wks
              FROM (
                       SELECT style_cd
                            , prdt_nm
                            , parent_prdt_kind_nm
                            , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN sale_nml_sale_amt_cns + sale_ret_sale_amt_cns ELSE 0 END                    AS week_sale_amt_cy
                            , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN sale_nml_qty_cns + sale_ret_qty_cns ELSE 0 END                              AS week_sale_qty_cy
                            , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN ac_stor_qty_kor ELSE 0 END                                                  AS ac_stor_qty_cy
                            , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN ac_sale_nml_qty_cns + ac_sale_ret_qty_cns ELSE 0 END                        AS ac_sale_qty_cy
                            , CASE WHEN end_dt = '{para_end_dt_this_week}' THEN stock_qty ELSE 0 END                                                        AS stock_qty
                            , CASE WHEN end_dt BETWEEN '{para_end_dt_this_week}' - 3 * 7 AND '{para_end_dt_this_week}' THEN sale_nml_qty_cns + sale_ret_qty_cns ELSE 0 END AS sale_qty_4wks
                       FROM prcs.db_scs_w a,
                            prcs.db_prdt b
                       WHERE a.brd_cd = b.brd_cd
                         AND a.prdt_cd = b.prdt_cd
                         AND a.brd_cd = '{para_brand}'
                         AND a.sesn IN ({para_season})
                         AND cat_nm = '{para_category}'
                         AND sub_cat_nm IN ({para_sub_category})
                         AND adult_kids_nm = '{para_adult_kids}'
                         AND end_dt BETWEEN '{para_end_dt_this_week}' - 7 * 3 AND '{para_end_dt_this_week}'
                   ) a1
              GROUP BY style_cd)
SELECT a.*, b.url
FROM (
         SELECT style_cd,
                prdt_nm,
                week_sale_amt_cy,
                CASE WHEN SUM(week_sale_amt_cy) OVER () = 0 THEN 0 ELSE ROUND(week_sale_amt_cy::NUMERIC / SUM(week_sale_amt_cy) OVER () * 100) END AS week_ratio,
                week_sale_qty_cy,
                ac_stor_qty_cy,
                ac_sale_qty_cy,
                stock_qty,
                sale_qty_4wks,
                CASE
                    WHEN parent_prdt_kind_nm = '의류' AND week_sale_qty_cy != 0 THEN ROUND(stock_qty::NUMERIC / week_sale_qty_cy)
                    WHEN parent_prdt_kind_nm = 'ACC' AND sale_qty_4wks != 0 THEN ROUND(stock_qty::NUMERIC / (1.0*sale_qty_4wks/4))
                    ELSE stock_qty
                    END                                                                                                                            AS woi,
                CASE WHEN ac_stor_qty_cy = 0 THEN 0 ELSE ROUND(ac_sale_qty_cy::NUMERIC / ac_stor_qty_cy * 100) END                                 AS sale_rate
         FROM main
     ) a
         LEFT OUTER JOIN prcs.db_style_img b ON (a.style_cd = b.style_cd AND b.default_yn = true)
ORDER BY week_sale_qty_cy DESC
        """.format(
            para_brand=self._para['para_brand'],
            para_season=self._para['para_season'],
            para_season_py=self._para['para_season_py'],
            para_season_py2=self._para['para_season_py2'],
            para_category=self._para['para_category'],
            para_sub_category=self._para['para_sub_category'],
            para_adult_kids=self._para['para_adult_kids'],
            para_start_dt=self.get_last_sunday(self._para['para_start_dt']),
            para_end_dt=self.get_last_sunday(self._para['para_end_dt']),
            para_end_dt_this_week=self.get_last_sunday(self._para['para_end_dt_this_week']),
        )

        return query
