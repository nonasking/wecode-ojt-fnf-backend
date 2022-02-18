from django.views import View
from django.http import JsonResponse

from utils.get_tuple import get_tuple
from utils.get_last_sunday import get_last_sunday
from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData


class Top20SummaryView(View):
    def __init__(self):
        self.column_descriptions = [
            ["cls", "구분", False], 
            ["sale_amt_kor_ttl", "판매액", "money"], 
            ["ratio", "매출비중", False], 
            ["asp", "평균판매가", "money"], 
            ["sale_qty_kor_ttl", "한국판매수", "money"],  
            ["stock_qty_kor", "재고수량", "money"], 
            ["woi", "재고주수", False],
        ]
    def get_query(self, *args, **kwargs):
        query = """
WITH main AS (
    SELECT style_cd
         , sale_amt
         , sale_qty
         , stock_qty
         , SUM(sale_amt) OVER () AS ttl_sale_amt
         , row_number() OVER (order by sale_amt desc) AS rank
    FROM (
        SELECT b.style_cd
             , SUM(a.sale_nml_sale_amt_cns+a.sale_ret_sale_amt_cns) sale_amt
             , SUM(a.sale_nml_qty_cns+a.sale_ret_qty_cns) sale_qty
             , SUM(a.stock_qty) stock_qty
        FROM prcs.db_scs_w a, prcs.db_prdt b
        WHERE a.prdt_cd = b.prdt_cd
        AND a.end_dt = '{para_end_dt}'
        AND b.brd_cd = '{para_brand}'
        AND sub_cat_nm IN {para_sub_category}
        AND domain1_nm IN {para_domain}
        AND (a.sesn||'_'||sesn_sub_nm) IN {para_season}
        AND item IN {para_item}
        AND (prdt_nm LIKE '%{para_search_keyword}%' OR style_cd LIKE '%{para_search_keyword}%')
        AND adult_kids_nm IN {para_adult_kids}
        GROUP BY b.style_cd
    )a
)
SELECT cls
     , sale_amt_kor_ttl / 1000000              AS sale_amt_kor_ttl
     , CASE WHEN ttl_sale_amt = 0 THEN 0 ELSE ROUND(sale_amt_kor_ttl::NUMERIC / ttl_sale_amt * 100)  END AS ratio
     , asp
     , sale_qty_kor_ttl
     , stock_qty_kor
     , CASE WHEN sale_qty_kor_ttl =0 THEN 0 ELSE ROUND(stock_qty_kor / sale_qty_kor_ttl) END AS woi
FROM (
    SELECT 0 rk
         , 'Top5'                           AS cls
         , SUM(sale_amt)                    AS sale_amt_kor_ttl
         , CASE WHEN SUM(sale_qty) = 0 THEN 0 ELSE SUM(sale_amt) / SUM(sale_qty) END    AS asp
         , SUM(sale_qty)                    AS sale_qty_kor_ttl
         , SUM(stock_Qty)                   AS stock_qty_kor
         , MAX(ttl_sale_amt)                AS ttl_sale_amt
    FROM main
    WHERE rank BETWEEN 1 AND 5
    UNION ALL
    SELECT 1 rk
         , 'Top20'                          AS cls
         , SUM(sale_amt)                    AS sale_amt_kor_ttl
         , CASE WHEN SUM(sale_qty) = 0 THEN 0 ELSE SUM(sale_amt) / SUM(sale_qty) END    AS asp
         , SUM(sale_qty)                    AS sale_qty_kor_ttl
         , SUM(stock_Qty)                   AS stock_qty_kor
         , MAX(ttl_sale_amt)                AS ttl_sale_amt
    FROM main
    WHERE rank BETWEEN 1 AND 20
    UNION ALL
    SELECT 2 rk
         , 'Top50'                          AS cls
         , SUM(sale_amt)                    AS sale_amt_kor_ttl
         , CASE WHEN SUM(sale_qty) = 0 THEN 0 ELSE SUM(sale_amt) / SUM(sale_qty) END    AS asp
         , SUM(sale_qty)                    AS sale_qty_kor_ttl
         , SUM(stock_Qty)                   AS stock_qty_kor
         , MAX(ttl_sale_amt)                AS ttl_sale_amt
    FROM main
    WHERE rank BETWEEN 1 AND 50
    UNION ALL
    SELECT 3 rk
         , 'Top100'                         AS cls
         , SUM(sale_amt)                    AS sale_amt_kor_ttl
         , CASE WHEN SUM(sale_qty) = 0 THEN 0 ELSE SUM(sale_amt) / SUM(sale_qty) END    AS asp
         , SUM(sale_qty)                    AS sale_qty_kor_ttl
         , SUM(stock_Qty)                   AS stock_qty_kor
         , MAX(ttl_sale_amt)                AS ttl_sale_amt
    FROM main
    WHERE rank BETWEEN 1 AND 100
    UNION ALL
    SELECT 4 rk
         , 'Total'                          AS cls
         , sum(sale_amt)                    AS sale_amt_kor_ttl
         , case when sum(sale_qty) = 0 then 0 else sum(sale_amt) / sum(sale_qty) end    as asp
         , sum(sale_qty)                    as sale_qty_kor_ttl
         , sum(stock_Qty)                   as stock_qty_kor
         , max(ttl_sale_amt)                as ttl_sale_amt
    from main
)a
order by rk
        """.format(
            para_brand=kwargs['brand'],
            para_sub_category=kwargs['sub_category'],
            para_domain=kwargs['domain'],
            para_item=kwargs['item'],
            para_season=kwargs['season'],
            para_search_keyword=kwargs['search_keyword'],
            para_adult_kids=kwargs['adult_kids'],
            para_end_dt=kwargs['end_date_this_week'],
        )

        return query

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            end_date_this_week = request.GET["end-date"]
            search_keyword = request.GET.get("search_keyword", None)
            adult_kids = request.GET["adult-kids"].split(",")
            item = request.GET["items"].split(",")
            domain = request.GET["domains"].split(",")
            season = request.GET["seasons"].split(",")
            sub_category = request.GET["subcategories"].split(",")
            connect = request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            adult_kids = get_tuple(adult_kids)
            item = get_tuple(item)
            domain = get_tuple(domain)
            season = get_tuple(season)
            sub_category = get_tuple(sub_category)

            if search_keyword is None:
                search_keyword = ""

            query = self.get_query(
                brand = brand,
                sub_category = sub_category,
                domain = domain,
                item = item,
                season = season,
                search_keyword = search_keyword,
                adult_kids = adult_kids,
                end_date_this_week = end_date_this_week,
            )

            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            columns = [
                {"title":item[0], 
                 "field":item[1],
                 "formatter":item[2],
                 "formatterParams":{"precision":False}} 
                for item in self.column_descriptions
            ]

            contents = data.to_dict("records")

            return JsonResponse({"message":"SUCCESS", "columns":columns, "data":contents},status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)


class Top20ListView(View):

    def __init__(self):
        self.column_descriptions=[
            ["ranking", "랭킹", False],
            ["rank_growth", "상승", False],
            ["repr_cd", "대표품번", False],
            ["image_name", "이미지", False],
            ["tag_price", "택가", "money"],
            ["discount", "할인율", False],
            ["prdt_nm", "제품명", False],
            ["sales12", "추이", False],
            ["sale_qty_kor_ttl", "수량", "money"],
            ["sale_qty_kor_retail", "국내", "money"],
            ["sale_qty_kor_duty","면세", "money"],
            ["sale_qty_kor_dutyrfwholesale", "RF도매", "money"],
            ["asp", "실판", "money"],
            ["sale_amt_kor_ttl", "판매액", "money"],
            ["ac_sale_qty_cns", "누적판매량", "money"],
            ["ac_stor_qty_kor", "누적입고량", "money"],
            ["wh_stock_qty_kor", "물류재고", "money"],
            ["stock_qty_kor", "총재고", "money"],
            ["woi", "재고주수", False],
            ["sale_rate", "판매율", False],
    ]

    def get_query(self, *args, **kwargs):
        query = """
with main as (
    select b.style_cd
         , case when max(parent_prdt_kind_nm) = '의류' then max(b.part_cd) else b.style_cd end as repr_cd
         , max(b.parent_prdt_kind_nm)                                                       as parent_prdt_kind_nm
         , max(b.prdt_nm)                                                                   as prdt_nm
         , max(tag_price)                                                                   as tag_price
         , a.end_dt                                                                         as end_dt
         , sum(sale_nml_sale_amt_cns + sale_ret_sale_amt_cns)                               as sale_amt
         , sum(sale_nml_tag_amt_cns + sale_ret_tag_amt_cns)                                 as sale_tag
         , sum(sale_nml_qty_cns + sale_ret_qty_cns)                                         as sale_qty
         , sum(sale_nml_qty_rtl + sale_ret_qty_rtl)                                         as sale_qty_rtl
         , sum(sale_nml_qty_notax + sale_ret_qty_notax)                                     as sale_qty_notax
         , sum(sale_nml_qty_rf + sale_ret_qty_rf)                                           as sale_qty_rfdome
         , sum(sale_nml_qty_rf + sale_ret_qty_rf + sale_nml_qty_notax + sale_ret_qty_notax) as sale_qty_dutyrfdome
         , sum(wh_stock_qty)                                                                as wh_stock_qty
         , sum(stock_qty)                                                                   as stock_qty
         , sum(ac_stor_qty_kor)                                                             as ac_stor_qty_kor
         , sum(ac_sale_nml_qty_cns + ac_sale_ret_qty_cns)                                   as ac_sale_qty_cns
    from prcs.db_scs_w a,
         prcs.db_prdt b
    where a.prdt_cd = b.prdt_cd
    and a.brd_cd = '{para_brand}'
    and end_dt between '{para_end_dt}'-7*12 and '{para_end_dt}'
    AND sub_cat_nm IN {para_sub_category}
    AND domain1_nm IN {para_domain}
    AND (a.sesn||'_'||sesn_sub_nm) IN {para_season}
    AND item IN {para_item}
    AND (prdt_nm LIKE '%{para_search_keyword}%' OR style_cd LIKE '%{para_search_keyword}%')
    AND adult_kids_nm IN {para_adult_kids}
    group by b.style_cd, a.end_dt
)
select a.ranking                                                                        as ranking
     , case
           when b.ranking is null or b.ranking = a.ranking then '-'
           when b.ranking > a.ranking then '↑' || b.ranking - a.ranking
           else '↓' || a.ranking - b.ranking
    end                                                                                 as rank_growth
     , nvl(b.ranking, 0) - a.ranking                                                    as rank_check
     , nvl(b.ranking, 0)                                                                as ranking_2wks
     , a.style_cd                                                                       as prdt_cd
     , repr_cd
     , a.prdt_nm                                                                        as prdt_nm
     , e.tag_price                                                                        as tag_price
     , d.url                                                                            as image_name
     , a.sale_amt / 1000000                                                           as sale_amt_kor_ttl
     , case when sale_tag =0 then 0 else round((1-(sale_amt::numeric /sale_tag))*100) end as discount
     , case when a.sale_qty = 0 then 0 else round(a.sale_amt::numeric / a.sale_qty) end as asp
     , case when c.sale12 = '0' then '' else c.sale12 end                               as sales12
     , a.sale_qty                                                                       as sale_qty_kor_ttl
     , a.sale_qty_rtl                                                                   as sale_qty_kor_retail
     , a.sale_qty_notax                                                                 as sale_qty_kor_duty
     , a.sale_qty_rfdome                                                                as sale_qty_kor_rfwholesale
     , a.sale_qty_dutyrfdome                                                            as sale_qty_kor_dutyrfwholesale
     , a.wh_stock_qty                                                                   as wh_stock_qty_kor
     , a.stock_qty                                                                      as stock_qty_kor
     , sale_qty_4wk_avg
     , ac_sale_qty_cns
     , ac_stor_qty_kor
     , case
           when parent_prdt_kind_nm = 'ACC' and sale_qty_4wk_avg != 0 then round(a.stock_qty::numeric / sale_qty_4wk_avg)
           when parent_prdt_kind_nm != 'ACC' and a.sale_qty != 0 then round(a.stock_qty::numeric / a.sale_qty)
           else a.stock_qty end                                                         as woi
     , case
           when ac_stor_qty_kor != 0 then round(ac_sale_qty_cns::numeric / ac_stor_qty_kor * 100)
           else 0 end                                                                  as sale_rate
from (
    select style_cd
          , repr_cd
          , row_number() over ( order by sale_amt desc) ranking
          , parent_prdt_kind_nm
          , tag_price
          , prdt_nm
          , sale_amt
          , sale_tag
          , sale_qty
          , sale_qty_rtl
          , sale_qty_notax
          , sale_qty_rfdome
          , sale_qty_dutyrfdome
          , wh_stock_qty
          , stock_qty
          , ac_stor_qty_kor
          , ac_sale_qty_cns
    from main
    where end_dt = '{para_end_dt}'
) a
left join (
     select style_cd
          , row_number() over ( order by sale_amt desc) ranking
    from main
    where end_dt = '{para_end_dt}'-7
) b
on a.style_cd = b.style_cd
left join (
     select style_cd, listagg(sale_qty,',') within group ( order by end_dt ) sale12
    from (
        select style_cd, end_dt, sum(sale_qty) sale_qty
        from main
        group by style_cd, end_dt
    )a
    group by style_cd
) c
on a.style_cd = c.style_cd
 left join (select style_cd, round(sum(sale_qty) / 4) as sale_qty_4wk_avg
            from main
            where end_dt between '{para_end_dt}' - 7 * 3 and '{para_end_dt}'
            group by style_cd) z
           on a.style_cd = z.style_cd
left join prcs.db_style_img d
on a.style_cd = d.style_cd
and d.default_yn = true
left join ( select style_cd, listagg(tag_price, ',') within group ( order by tag_price desc ) tag_price
            from (select distinct style_cd, tag_price from prcs.db_prdt)a
            group by style_cd) e on (a.style_cd = e.style_cd)
order by 1
limit {para_rank_limit}
        """.format(
            para_brand=kwargs['brand'],
            para_sub_category=kwargs['sub_category'],
            para_domain=kwargs['domain'],
            para_item=kwargs['item'],
            para_season=kwargs['season'],
            para_search_keyword=kwargs['search_keyword'],
            para_adult_kids=kwargs['adult_kids'],
            para_rank_limit=kwargs['rank_limit'],
            para_end_dt=kwargs["end_date_this_week"],
        )

        return query

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            end_date_this_week = request.GET["end-date"]
            search_keyword = request.GET.get("search_keyword", None)
            rank_limit = request.GET.get("limit", 200)
            adult_kids = request.GET["adult-kids"].split(",")
            item = request.GET["items"].split(",")
            domain = request.GET["domains"].split(",")
            season = request.GET["seasons"].split(",")
            sub_category = request.GET["subcategories"].split(",")
            connect = request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            adult_kids = get_tuple(adult_kids)
            item = get_tuple(item)
            domain = get_tuple(domain)
            season = get_tuple(season)
            sub_category = get_tuple(sub_category)

            if search_keyword is None:
                search_keyword = ""

            query = self.get_query(
                brand = brand,
                sub_category = sub_category,
                domain = domain,
                item = item,
                season = season,
                search_keyword = search_keyword,
                adult_kids = adult_kids,
                end_date_this_week = end_date_this_week,
                rank_limit = rank_limit
            )

            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            columns = [
                {"title":item[0],
                 "field":item[1],
                 "formatter":item[2],
                 "formatterParams":{"precision":False}}
                for item in self.column_descriptions
            ]

            column_list = [item[0] for item in self.column_descriptions]
            contents = data[column_list].to_dict("records")

            return JsonResponse({"message":"SUCCESS", "columns":columns, "data":contents}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)


class Top20TotalSummaryView(View):

    def __init__(self):
        self.column_descriptions = [
            ["cls", "총합계", False],
            ["sale_amt_kor_ttl", "판매액", "money"],
            ["sale_qty_kor_ttl", "판매량", "money"],
            ["asp", "실판가", "money"], 
            ["sale_qty_kor_retail", "국내", "money"],
            ["sale_qty_kor_duty", "면세", "money"],
            ["sale_qty_kor_rfwholesale", "RF도매", "money"],
            ["wh_stock_qty_kor", "물류재고", "money"],
            ["stock_qty_kor", "총재고", "money"],
            ["woi", "재고주수", False],
        ]

    def get_query(self, *args, **kwargs):
        query = """
select '총합계' cls
     , sale_amt / 1000000     as sale_amt_kor_ttl
     , sale_qty                as sale_qty_kor_ttl
     , case when sale_qty = 0 then 0 else sale_amt / sale_qty end asp
     , sale_qty_rtl            as sale_qty_kor_retail
     , sale_qty_notax        as sale_qty_kor_duty
     , sale_qty_rfdome        as sale_qty_kor_rfwholesale
     , sale_qty_dutyrfdome    as sale_qty_kor_dutyrfwholesale
     , wh_stock_qty            as wh_stock_qty_kor
     , stock_qty            as stock_qty_kor
     , case when sale_qty = 0 then stock_qty else stock_qty / sale_qty end woi
from (
    select sum(sale_nml_sale_amt_cns+sale_ret_sale_amt_cns) sale_amt
         , sum(sale_nml_qty_cns+sale_ret_qty_cns) sale_qty
         , sum(sale_nml_qty_rtl+sale_ret_qty_rtl) sale_qty_rtl
         , sum(sale_nml_qty_notax+sale_ret_qty_notax) sale_qty_notax
         , sum(sale_nml_qty_rf+sale_ret_qty_rf+sale_nml_qty_dome+sale_ret_qty_dome) sale_qty_rfdome
         , sum(sale_nml_qty_rf+sale_ret_qty_rf+sale_nml_qty_dome+sale_ret_qty_dome+sale_nml_qty_notax+sale_ret_qty_notax) sale_qty_dutyrfdome
         , sum(wh_stock_qty)    as wh_stock_qty
         , sum(stock_qty) as stock_qty
    from prcs.db_scs_w a, prcs.db_prdt b
    where a.prdt_cd = b.prdt_cd
    and b.brd_cd = '{para_brand}'
    and a.end_dt = '{para_end_dt}'
    AND sub_cat_nm IN {para_sub_category}
    AND domain1_nm IN {para_domain}
    AND (a.sesn||'_'||sesn_sub_nm) IN {para_season}
    AND item IN {para_item}
    AND (prdt_nm LIKE '%{para_search_keyword}%' OR style_cd LIKE '%{para_search_keyword}%')
    AND adult_kids_nm IN {para_adult_kids}
)a
        """.format(
            para_brand=kwargs['brand'],
            para_sub_category=kwargs['sub_category'],
            para_domain=kwargs['domain'],
            para_item=kwargs['item'],
            para_season=kwargs['season'],
            para_search_keyword=kwargs['search_keyword'],
            para_adult_kids=kwargs['adult_kids'],
            para_rank_limit=kwargs['rank_limit'],
            para_end_dt=kwargs["end_date_this_week"],
        )
        
        return query

    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            brand = request.GET["brand"]
            end_date_this_week = request.GET["end-date"]
            search_keyword = request.GET.get("search_keyword", None)
            rank_limit = request.GET.get("limit", 200)
            adult_kids = request.GET["adult-kids"].split(",")
            item = request.GET["items"].split(",")
            domain = request.GET["domains"].split(",")
            season = request.GET["seasons"].split(",")
            sub_category = request.GET["subcategories"].split(",")
            connect = request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            adult_kids = get_tuple(adult_kids)
            item = get_tuple(item)
            domain = get_tuple(domain)
            season = get_tuple(season)
            sub_category = get_tuple(sub_category)

            if search_keyword is None:
                search_keyword = ""

            query = self.get_query(
                brand = brand,
                sub_category = sub_category,
                domain = domain,
                item = item,
                season = season,
                search_keyword = search_keyword,
                adult_kids = adult_kids,
                end_date_this_week = end_date_this_week,
                rank_limit = rank_limit
            )

            redshift_data = RedshiftData(connect, query)
            data = redshift_data.get_data()

            if data is None:
                return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

            columns = [
                {"title":item[0],
                 "field":item[1],
                 "formatter":item[2],
                 "formatterParams":{"precision":False}}
                for item in self.column_descriptions
            ]

            column_list = [item[0] for item in self.column_descriptions]
            contents = data[column_list].to_dict("records")

            return JsonResponse({"message":"SUCCESS", "columns":columns, "data":contents}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
