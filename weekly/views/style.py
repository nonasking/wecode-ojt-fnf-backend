import pandas as pd

from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday
from utils.get_previous_season import get_previous_season
from utils.check_item import check_keys_in_dictionary


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
                         AND a.sesn IN {para_season}
                         AND cat_nm = '{para_category}'
                         AND sub_cat_nm IN {para_sub_category}
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
            para_brand=kwargs['brand'],
            para_season=kwargs['season'],
            para_category=kwargs['category'],
            para_sub_category = kwargs['sub_category'],
            para_adult_kids = kwargs['adult_kids'],
            para_end_dt_this_week = kwargs['end_date_this_week'],
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
                 end_date_this_week = end_date_this_week,
            )

            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            column = ["url", "week_sale_qty_cy", "stock_qty", "woi", "sale_rate", "week_ratio", "prdt_nm", "style_cd"]
            result=data[column].to_dict("records")
            '''
            result = [
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220117/3AJKB0121-50BKS-58411266417270009.png",
                    "week_sale_qty_cy": 643,
                    "stock_qty": 1718,
                    "woi": 3.0,
                    "sale_rate": 58.0,
                    "week_ratio": 42.0,
                    "prdt_nm": "베이직 자켓",
                    "style_cd": "3AJ21"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20211230/3AJPB0321-50BKS-56872818707980652.png",
                    "week_sale_qty_cy": 625,
                    "stock_qty": 5693,
                    "woi": 9.0,
                    "sale_rate": 31.0,
                    "week_ratio": 27.0,
                    "prdt_nm": "뉴핏 MLB 점퍼",
                    "style_cd": "3A21"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220104/3AJPB0121-50GNS-57275690478386292.png",
                    "week_sale_qty_cy": 116,
                    "stock_qty": 562,
                    "woi": 5.0,
                    "sale_rate": 59.0,
                    "week_ratio": 6.0,
                    "prdt_nm": "베이직 야구점퍼",
                    "style_cd": "3AJ"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220124/3AWJB0121-50BKS-59014582214539115.png",
                    "week_sale_qty_cy": 112,
                    "stock_qty": 6002,
                    "woi": 54.0,
                    "sale_rate": 3.0,
                    "week_ratio": 3.0,
                    "prdt_nm": "베이직 바람막이",
                    "style_cd": "3A121"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220110/3AWJB0221-50BKS-57812473495692451.png",
                    "week_sale_qty_cy": 84,
                    "stock_qty": 2302,
                    "woi": 27.0,
                    "sale_rate": 10.0,
                    "week_ratio": 3.0,
                    "prdt_nm": "베이직 아노락",
                    "style_cd": "3AWJB"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220110/3AWJM0121-50CRS-57812478540233229.png",
                    "week_sale_qty_cy": 80,
                    "stock_qty": 4724,
                    "woi": 59.0,
                    "sale_rate": 8.0,
                    "week_ratio": 3.0,
                    "prdt_nm": "모노그램 바람막이",
                    "style_cd": "3AW"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220110/3FWJB0121-50BKS-57812543981169314.png",
                    "week_sale_qty_cy": 62,
                    "stock_qty": 2214,
                    "woi": 36.0,
                    "sale_rate": 7.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "여성 베이직 셋업",
                    "style_cd": "B0121"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220105/3AWJM0221-50BGS-57365119517067882.png",
                    "week_sale_qty_cy": 58,
                    "stock_qty": 1560,
                    "woi": 27.0,
                    "sale_rate": 23.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "모노그램 바람막이",
                    "style_cd": "3A221"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220106/3AWJS0221-50BKS-57475639787988900.png",
                    "week_sale_qty_cy": 50,
                    "stock_qty": 3054,
                    "woi": 61.0,
                    "sale_rate": 3.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "씸볼 바람막이",
                    "style_cd": "S0221"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220110/3AJPH0121-50BKS-57811937043241509.png",
                    "week_sale_qty_cy": 40,
                    "stock_qty": 1292,
                    "woi": 32.0,
                    "sale_rate": 7.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "베이스볼 점퍼",
                    "style_cd": "H0121"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220105/3FWJM0221-50BGS-57365121086491935.png",
                    "week_sale_qty_cy": 40,
                    "stock_qty": 1591,
                    "woi": 40.0,
                    "sale_rate": 18.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "여성 바람막이",
                    "style_cd": "3F0221"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220105/3LWJM0321-50BGS-57365126140435092.png",
                    "week_sale_qty_cy": 37,
                    "stock_qty": 1883,
                    "woi": 51.0,
                    "sale_rate": 12.0,
                    "week_ratio": 2.0,
                    "prdt_nm": "남성 바람막이",
                    "style_cd": "M0321"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220106/3AJPM0121-50BGS-57475635093751833.png",
                    "week_sale_qty_cy": 25,
                    "stock_qty": 2832,
                    "woi": 113.0,
                    "sale_rate": 5.0,
                    "week_ratio": 1.0,
                    "prdt_nm": "MLB 점퍼",
                    "style_cd": "321"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220110/3FWJB0221-50BKS-57812547062353124.png",
                    "week_sale_qty_cy": 24,
                    "stock_qty": 1736,
                    "woi": 72.0,
                    "sale_rate": 3.0,
                    "week_ratio": 1.0,
                    "prdt_nm": "여성아노락",
                    "style_cd": "J221"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220105/3AWJS0321-50WHS-57378610559498840.png",
                    "week_sale_qty_cy": 20,
                    "stock_qty": 2055,
                    "woi": 103.0,
                    "sale_rate": 11.0,
                    "week_ratio": 1.0,
                    "prdt_nm": "아노락",
                    "style_cd": "3AWJ"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220128/3AWJA0121-50BKS-59365290722411454.png",
                    "week_sale_qty_cy": 8,
                    "stock_qty": 2018,
                    "woi": 252.0,
                    "sale_rate": 1.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "애슬레저 바람막이",
                    "style_cd": "3AWJA"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220106/3AJPL0121-50BLS-57451313758815087.png",
                    "week_sale_qty_cy": 6,
                    "stock_qty": 994,
                    "woi": 166.0,
                    "sale_rate": 5.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "베이스볼 점퍼",
                    "style_cd": "L0121"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220208/3FWJB0421-50BKS-60328982620510166.png",
                    "week_sale_qty_cy": 0,
                    "stock_qty": 0,
                    "woi": 0.0,
                    "sale_rate": 0.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "컬러블럭 바람막이 (홑겹)",
                    "style_cd": "0421"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220207/3AWJB0621-50CRS-60223404411180775.png",
                    "week_sale_qty_cy": 0,
                    "stock_qty": 0,
                    "woi": 0.0,
                    "sale_rate": 0.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "베이직 바람막이 (홑겹)",
                    "style_cd": "0621"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220207/3AWJB0521-50CRS-60222135447027369.png",
                    "week_sale_qty_cy": 0,
                    "stock_qty": 0,
                    "woi": 0.0,
                    "sale_rate": 0.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "베이직 바람막이 (홑겹)",
                    "style_cd": "3A"
                },
                {
                "url": null,
                "week_sale_qty_cy": 0,
                "stock_qty": 0,
                "woi": 0.0,
                "sale_rate": 0.0,
                "week_ratio": 0.0,
                "prdt_nm": " 바람막이",
                    "style_cd": "3A21"
                },
                {
                    "url": "http://static.mlb-korea.com/images/goods/thnail/m/20220203/3FWJM0121-50BKS-59895595451103942.png",
                    "week_sale_qty_cy": 0,
                    "stock_qty": 2861,
                    "woi": 2861.0,
                    "sale_rate": 0.0,
                    "week_ratio": 0.0,
                    "prdt_nm": "여성 바람막이 셋업",
                    "style_cd": "121"
                }
            ]
            return JsonResponse({"message":"success", "data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
