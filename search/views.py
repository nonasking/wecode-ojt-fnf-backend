import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql

from django.http import JsonResponse
from django.views import View

from utils.get_tuple import get_tuple
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.get_last_sunday import get_last_sunday

# 브랜드별 검색량 표(당해/전년/전년비) (주간/선택기간)
class SearchCountTableView(View):

    @connect_redshift
    def get(self, request, term, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date_this_week = request.GET["weekly-date"]
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            sub_category = get_tuple(sub_category) 

            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kids = adult_kids,
                start_date = start_date,
                end_date_this_week = end_date_this_week
            )
            
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if term == 'weekly':
                data = data[data['term_cls']=='주간']
            
            elif term == 'selected-period':
                data = data[data['term_cls']=='선택기간']
            
            data = data[data.columns.difference(['term_cls'])]
            result = data.to_dict('records')
            
            return JsonResponse({"message":"success", "data":result}, status=200)
    

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_query(self, *args, **kwargs):
        query = """

WITH rds AS (
    SELECT end_dt,
           cat_nm,
           sub_cat_nm,
           comp_type,
           comp_brd_nm,
           kwd_nm,
           srch_cnt
    FROM (
             SELECT DISTINCT b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT DISTINCT brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND sub_cat_nm in {para_sub_category}                     
                     AND adult_kids_nm = '{para_adult_kids}'
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
    AND end_dt BETWEEN '{para_start_dt}'-364 AND '{para_end_dt_this_week}'
)
SELECT *
FROM (
         SELECT '선택기간'                                                                                                   AS term_cls
              , comp_brd_nm                                                                                              AS competitor
              , SUM(srch_cnt_cy)                                                                                         AS seach_qty_cy
              , SUM(srch_cnt_py)                                                                                         AS seach_qty_py
              , case WHEN SUM(srch_cnt_py) = 0 THEN 0 ELSE ROUND(SUM(srch_cnt_cy)::numeric / SUM(srch_cnt_py) * 100) end AS growth
         FROM (
                  SELECT comp_brd_nm, comp_type, srch_cnt AS srch_cnt_cy, 0 AS srch_cnt_py
                  FROM rds
                  WHERE end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
                  UNION ALL
                  SELECT comp_brd_nm, comp_type, 0 AS srch_cnt_cy, srch_cnt AS srch_cnt_py
                  FROM rds
                  WHERE end_dt BETWEEN '{para_start_dt}'- 364 AND '{para_end_dt_this_week}' - 364
              ) a
         GROUP BY comp_brd_nm
         UNION ALL
         SELECT '주간'                                                                                                     AS term_cls
              , comp_brd_nm                                                                                              AS competitor
              , SUM(srch_cnt_cy)                                                                                         AS seach_qty_cy
              , SUM(srch_cnt_py)                                                                                         AS seach_qty_py
              , case WHEN SUM(srch_cnt_py) = 0 THEN 0 ELSE ROUND(SUM(srch_cnt_cy)::numeric / SUM(srch_cnt_py) * 100) end AS growth
         FROM (
                  SELECT comp_brd_nm, comp_type, srch_cnt AS srch_cnt_cy, 0 AS srch_cnt_py
                  FROM rds
                  WHERE end_dt = '{para_end_dt_this_week}'
                  UNION ALL
                  SELECT comp_brd_nm, comp_type, 0 AS srch_cnt_cy, srch_cnt AS srch_cnt_py
                  FROM rds
                  WHERE end_dt = '{para_end_dt_this_week}' - 364
              ) a
         GROUP BY comp_brd_nm
     ) a
ORDER BY term_cls, seach_qty_cy DESC


        """. \
            format(
            para_brand=kwargs["brand"],
            para_category=kwargs["category"],
            para_sub_category=kwargs["sub_category"],
            para_adult_kids=kwargs["adult_kids"],
            para_start_dt=kwargs["start_date"],
            para_end_dt_this_week=kwargs["end_date_this_week"]
        )
        return query

# 일반 검색어/자사 검색어 추이 차트(당해/전년)
class SearchCountTimeSeriesOverallView(View):

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["weekly-date"]
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect

            end_date = get_last_sunday(end_date)
            end_date_this_week = get_last_sunday(end_date_this_week)

            sub_category = get_tuple(sub_category) 

            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kids = adult_kids,
                start_date = start_date,
                end_date = end_date,
                end_date_this_week = end_date_this_week
            )
        
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()
            
            # 키 값 프론트와 맞추기(overall, self?)
            overall_result = self.filter_search_count(data,'일반')
            self_result = self.filter_search_count(data,'자사')

            return JsonResponse({"message":"success", "일반":overall_result, '자사':self_result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400) 

    def get_query(self, *args, **kwargs):
        query = """

WITH SEARCH AS (
    SELECT end_dt,
           comp_type,
           comp_brd_nm,
           srch_cnt AS srch_cnt_cy,
           0        AS srch_cnt_py
    FROM (
             SELECT DISTINCT b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT DISTINCT brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND sub_cat_nm in {para_sub_category}
                     AND adult_kids_nm = '{para_adult_kids}'
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
      AND b.end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
    UNION ALL
    SELECT end_dt + 364 AS end_dt,
           comp_type,
           comp_brd_nm,
           0            AS srch_cnt_cy,
           srch_cnt     AS srch_cnt_py
    FROM (
             SELECT DISTINCT b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT DISTINCT brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND sub_cat_nm in {para_sub_category}
                     AND adult_kids_nm = '{para_adult_kids}'
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
      AND b.end_dt BETWEEN '{para_start_dt}'-364 AND '{para_end_dt}' -364
)
SELECT TO_CHAR(end_dt, 'yy.mm.dd') AS end_dt
     , comp_brd_nm
     , comp_type
     , SUM(srch_cnt_cy)            AS search_qty_cy
     , SUM(srch_cnt_py)            AS search_qty_py
FROM SEARCH
GROUP BY end_dt
       , comp_brd_nm
       , comp_type
ORDER BY 1

        """. \
            format(
            para_brand=kwargs["brand"],
            para_category=kwargs["category"],
            para_sub_category=kwargs["sub_category"],
            para_adult_kids=kwargs["adult_kids"],
            para_start_dt=kwargs["start_date"],
            para_end_dt=kwargs["end_date"],
            para_end_dt_this_week=kwargs["end_date_this_week"]
        )
        return query
    
    def filter_search_count(self, data, comp_type):
        data = data[data['comp_type']==f'{comp_type}']
        data = data[['end_dt', 'search_qty_cy', 'search_qty_py']]
        return data.to_dict('records')
    
# 경쟁사 검색어 추이 차트
class SearchCountCompetitorTimeSeriesView(View):

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            category = request.GET["categories"]
            adult_kids = request.GET["adult-kids"]
            start_date = request.GET["start-date"]
            end_date = request.GET["end-date"]
            end_date_this_week = request.GET["weekly-date"]
            sub_category = request.GET.getlist("subcategories",None)
            connect =request.connect

            end_date = get_last_sunday(end_date)
            end_date_this_week = get_last_sunday(end_date_this_week)

            sub_category = get_tuple(sub_category) 

            query = self.get_query(
                brand = brand,
                category = category,
                sub_category = sub_category,
                adult_kids = adult_kids,
                start_date = start_date,
                end_date = end_date,
                end_date_this_week = end_date_this_week,
            )
            
            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            # 상위 5개 회사 추리기
            five_competitors_data = data['ttl_qty_cy'].groupby(data['comp_brd_nm']).max()
            five_competitors_list = five_competitors_data.sort_values(ascending=False)[:5].index.tolist()  
            
            brand_name = self.get_brand_name(data)
            
            if brand_name not in five_competitors_list:
                competitors_list = five_competitors_list + [brand_name]
            
            competitors_data = data[data['comp_brd_nm'].isin(competitors_list)]            
            
            competitors_data = competitors_data.pivot('end_dt', 'comp_brd_nm', 'search_qty_cy')
            competitors_data.columns = competitors_data.columns.values
            competitors_data.reset_index(inplace=True)
            
            result = competitors_data.to_dict('records')

            return JsonResponse({"message":"success", "data":result}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_brand_name(self, data):
        brand_data = data.loc[data['comp_type'] == '자사']
        result = brand_data.iloc[0]['comp_brd_nm']
        return result

    def get_query(self, *args, **kwargs):
        query = """

with SEARCH AS (
    SELECT end_dt,
           comp_type,
           comp_brd_nm,
           srch_cnt AS srch_cnt_cy
    FROM (
             SELECT DISTINCT b.cat_nm, b.sub_cat_nm, comp_type, comp_brd_nm, kwd_nm
             FROM (SELECT DISTINCT brd_cd, cat_nm, sub_cat_nm
                   FROM prcs.db_prdt a
                   WHERE 1 = 1
                     AND brd_cd = '{para_brand}'
                     AND cat_nm = '{para_category}'
                     AND sub_cat_nm in {para_sub_category}
                     AND adult_kids_nm = '{para_adult_kids}'
                  ) a,
                  prcs.db_srch_kwd_naver_mst b
             WHERE (a.cat_nm = b.sub_cat_nm
                 OR a.sub_cat_nm = b.sub_cat_nm)
               AND a.brd_cd = b.brd_cd
               AND adult_kids = '{para_adult_kids}'
               AND comp_type != '일반'
         ) a,
         prcs.db_srch_kwd_naver_w b
    WHERE a.kwd_nm = b.kwd
      AND b.end_dt BETWEEN '{para_start_dt}' AND '{para_end_dt_this_week}'
)

SELECT end_dt
     , comp_brd_nm
     , comp_type
     , search_qty_cy
     , SUM(search_qty_cy) OVER (partition by comp_brd_nm) AS ttl_qty_cy
FROM (
         SELECT TO_CHAR(end_dt, 'yy.mm.dd') AS end_dt
              , comp_brd_nm
              , comp_type
              , SUM(srch_cnt_cy)            AS search_qty_cy
         FROM SEARCH
         GROUP BY end_dt
                , comp_brd_nm
                , comp_type
         ORDER BY 1
     ) a
ORDER BY end_dt ASC, comp_brd_nm DESC
        

        """. \
            format(
            para_brand=kwargs["brand"],
            para_category=kwargs["category"],
            para_sub_category=kwargs["sub_category"],
            para_adult_kids=kwargs["adult_kids"],
            para_start_dt=kwargs["start_date"],
            para_end_dt=kwargs["end_date"],
            para_end_dt_this_week=kwargs["end_date_this_week"],
        )
        return query

