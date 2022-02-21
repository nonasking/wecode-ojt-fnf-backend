import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql

from django.http import JsonResponse
from django.views import View

from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.check_item import check_keys_in_dictionary
from utils.get_last_sunday import get_last_sunday

class SalesTrendView(View):
    @connect_redshift
    def get(self, request, type, *args, **kwargs):
        try:
            required_keys = ['brand', "product-cd", 'end-date']
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            end_date_this_week = request.GET["end-date"]
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            if type in ['korea', 'global'] :
                result = self.get_sales_trend_data(brand,product_cd,end_date_this_week,type,connect)
            elif type == 'ratio':
                result = self.get_sales_trend_ratio_data(brand,product_cd,end_date_this_week,connect)

            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_sales_trend_data(self, brand, product_cd, end_date_this_week, type, connect):
        sales_trend_data_query = """

select to_char(end_dt,'yy.mm.dd') as end_dt,
--        sum(sales_kor_cy)         as sales_kor_cy,
--        sum(sales_retail_cy)      as sales_retail_cy,
--        sum(sales_dutyrfwhole_cy) as sales_dutyrfwhole_cy,
--        sum(sales_chn_cy)         as sales_chn_cy,
--        sum(sales_gvl_cy)         as sales_gvl_cy,
       sum(qty_kor_cy)           as qty_kor_cy,
       sum(qty_retail_cy)        as qty_retail_cy,
       sum(qty_dutyrfwhole_cy)   as qty_dutyrfwhole_cy,
       sum(qty_chn_cy)           as qty_chn_cy,
       sum(qty_gvl_cy)           as qty_gvl_cy
from (
         select end_dt
--               , sale_nml_sale_amt_cns + sale_ret_sale_amt_cns as sales_kor_cy
--               , sale_nml_sale_amt_rtl + sale_ret_sale_amt_rtl as sales_retail_cy
--               , sale_nml_sale_amt_notax + sale_ret_sale_amt_notax
--              + sale_nml_sale_amt_dome + sale_ret_sale_amt_dome
--              + sale_nml_sale_amt_rf + sale_ret_sale_amt_rf    as sales_dutyrfwhole_cy
--               , sale_nml_sale_amt_chn + sale_ret_sale_amt_chn as sales_chn_cy
--               , sale_nml_sale_amt_gvl + sale_ret_sale_amt_gvl as sales_gvl_cy

              , sale_nml_qty_cns + sale_ret_qty_cns           as qty_kor_cy
              , sale_nml_qty_rtl + sale_ret_qty_rtl           as qty_retail_cy
              , sale_nml_qty_notax + sale_ret_qty_notax
             + sale_nml_qty_dome + sale_ret_qty_dome
             + sale_nml_qty_rf + sale_ret_qty_rf              as qty_dutyrfwhole_cy
              , sale_nml_qty_chn + sale_ret_qty_chn           as qty_chn_cy
              , sale_nml_qty_gvl + sale_ret_qty_gvl           as qty_gvl_cy
         from prcs.db_scs_w a,
              prcs.db_prdt b
         where a.brd_cd = b.brd_cd
           and a.prdt_cd = b.prdt_cd
           and a.brd_cd = '{brand}'
           and style_cd = '{product_cd}'
           and end_dt between '{end_date_this_week}' - 7 * 11 and '{end_date_this_week}'
     ) a
group by end_dt
order by end_dt asc

        """
        query = get_query(
            query = sales_trend_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_this_week = end_date_this_week,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()
        
        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
        
        if type == 'korea':
            data = data[['end_dt','qty_retail_cy','qty_dutyrfwhole_cy']]
        elif type == 'global':
            data = data[['end_dt','qty_kor_cy','qty_chn_cy','qty_gvl_cy']]

        result = data.to_dict('records')

        return result

    def get_sales_trend_ratio_data(self, brand, product_cd, end_date_this_week, connect):
        sales_trend_ratio_data_query = """

select sum(sales_retail_cy)/1000000      as sales_retail_cy,
       sum(sales_dutyrfwhole_cy)/1000000 as sales_dutyrfwhole_cy

from (
         select sale_nml_sale_amt_rtl + sale_ret_sale_amt_rtl as sales_retail_cy
              , sale_nml_sale_amt_notax + sale_ret_sale_amt_notax
             + sale_nml_sale_amt_dome + sale_ret_sale_amt_dome
             + sale_nml_sale_amt_rf + sale_ret_sale_amt_rf    as sales_dutyrfwhole_cy

         from prcs.db_scs_w a,
              prcs.db_prdt b
         where a.brd_cd = b.brd_cd
           and a.prdt_cd = b.prdt_cd
           and a.brd_cd = '{brand}'
           and style_cd = '{product_cd}'
           and end_dt = '{end_date_this_week}'
     ) a

        """
        query = get_query(
            query = sales_trend_ratio_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_this_week = end_date_this_week,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data['sales_retail_cy_ratio'] = round((data['sales_retail_cy']/(data['sales_retail_cy']+data['sales_dutyrfwhole_cy']))*100,1)
        data['sales_dutyrfwhole_cy_ratio'] = round((data['sales_dutyrfwhole_cy']/(data['sales_dutyrfwhole_cy']+data['sales_retail_cy']))*100,1)
        data.fillna(0, inplace=True)

        result = data.to_dict('records')

        return result

class WeeklyView(View):
    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ['brand', "product-cd", 'end-date']
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            end_date_this_week = request.GET["end-date"]
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            result = self.get_weekly_data(brand,product_cd,end_date_this_week,connect)

            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_weekly_data(self, brand, product_cd, end_date_this_week, connect):
        weekly_data_query = """
        
select to_char(end_dt,'yyyymmdd') as end_dt
     , sum(stor_qty_kor
          +stor_qty_chn
          +stor_qty_chn_bb
          +stor_qty_gvl
          +stor_qty_gvl_bb
          +stor_qty_etc)             as stor_qty_kor
     , sum(delv_nml_qty_chn
           +delv_ret_qty_chn
          +delv_nml_qty_chn_bb
           +delv_ret_qty_chn_bb
          +delv_nml_qty_gvl
           +delv_ret_qty_gvl
          +delv_nml_qty_gvl_bb
           +delv_ret_qty_gvl_bb)        as delv_qty_exp
        , sum(delv_nml_qty_wsl
          +delv_ret_qty_wsl)        as delv_qty_outlet
     , sum(sale_nml_qty_cns
          +sale_ret_qty_cns)        as sale_qty_kor_ttl
     , sum(sale_nml_qty_rtl
          +sale_ret_qty_rtl)        as sale_qty_kor_retail
     , sum(sale_nml_qty_notax
          +sale_ret_qty_notax)        as sale_qty_kor_duty
     , sum(sale_nml_qty_rf
          +sale_ret_qty_rf
          +sale_nml_qty_dome
          +sale_ret_qty_dome
          )                            as sale_qty_kor_rfwholesale    
     , sum(stock_qty)                as stock_qty_kor
from prcs.db_scs_w a, prcs.dw_prdt b
where a.prdt_cd = b.prdt_cd
and b.style_cd = '{product_cd}'
and b.brd_cd = '{brand}'
and a.end_dt between '{end_date_this_week}'-7*12 and '{end_date_this_week}'
group by a.end_dt
order by a.end_dt desc
        
        """
        query = get_query(
            query = weekly_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_this_week = end_date_this_week,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        result = data.to_dict('records')

        return result

class ChannelView(View):
    @connect_redshift
    def get(self, request, type, *args, **kwargs):
        try:
            required_keys = ['brand', "product-cd", 'end-date']
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            end_date_this_week = request.GET["end-date"]
            connect =request.connect

            end_date_this_week = get_last_sunday(end_date_this_week)

            if type == 'overall':
                result = self.get_overall_data(brand,product_cd,end_date_this_week,connect)
            elif type == 'shops':
                result = self.get_shops_data(brand,product_cd,end_date_this_week,connect)
            
            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_overall_data(self, brand, product_cd, end_date_this_week, connect):
        overall_query = """
        
select anal_dist_type_nm                                                                                          as type_zone_nm
     , shop_cnt                                                                                                   as shop_cnt
     , case when sale_qty = 0 then 0 else round(sale_amt::numeric / sale_qty) end                                 as asp
     , sale_qty                                                                                                   as sale_qty
     , stock_qty                                                                                                  as shop_stock_qty
     , case when sale_qty = 0 then stock_qty else round(stock_qty::numeric / sale_qty) end                        as woi
     , sale_amt / 1000000                                                                                         as sale_amt
     , case when sum(sale_amt) over () = 0 then 0 else round(sale_amt::numeric / sum(sale_amt) over () * 100) end as ratio
     , ac_sale_qty                                                                         as ac_sale_qty     
from (
         select anal_dist_type_nm
              , sum(shop_cnt)  as shop_cnt
              , sum(sale_qty)  as sale_qty
              , sum(sale_amt)  as sale_amt
              , sum(stock_qty) as stock_qty
              , sum(ac_sale_qty) as ac_sale_qty              
         from (
                  select max(c.anal_dist_type_nm) as anal_dist_type_nm
                       , a.shop_id
                       , case when sum(sale_nml_sale_amt + sale_ret_sale_amt) > 0 then 1 else 0 end as shop_cnt
                       , sum(sale_nml_qty + sale_ret_qty)                                           as sale_qty
                       , sum(sale_nml_sale_amt + sale_ret_sale_amt)                                 as sale_amt
                       , sum(a.sh_stock_qty)                                                        as stock_qty
                       , sum(ac_sale_nml_qty + ac_sale_ret_qty)                                     as ac_sale_qty
                  from prcs.db_sh_scs_w_rpt a,
                       prcs.dw_prdt b,
                       prcs.db_shop c
                  where a.prdt_cd = b.prdt_cd
                    and a.brd_cd = c.brd_cd
                    and a.shop_id = c.shop_id
                    and c.mng_type = 'A'
                    and c.anal_cntry = 'KO'
                    and c.shop_type = 'A'
                    and a.end_dt = '{end_date_this_week}'
                    and a.brd_cd = '{brand}'
                    and b.style_cd = '{product_cd}'
                  group by a.shop_id
                  having sum(sale_nml_sale_amt + sale_ret_sale_amt) != 0
              ) a
         group by anal_dist_type_nm
     ) a
order by sale_amt desc
        
        """
        query = get_query(
            query = overall_query,
            brand = brand,
            product_cd = product_cd,
            end_date_this_week = end_date_this_week,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        result = data.to_dict('records')

        return result
    
    def get_shops_data(self, brand, product_cd, end_date_this_week, connect):
        shops_query = """
        
select shop_id as                                                                          shopcode
     , shop_nm as                                                                          shop_nm
     , anal_dist_type_nm
     , case when sale_qty = 0 then 0 else round(sale_amt::numeric / sale_qty) end          asp
     , sale_qty
     , stock_qty
     , case when sale_qty = 0 then stock_qty else round(stock_qty::numeric / sale_qty) end woi
     ,ac_sale_qty
from (
    select a.shop_id
         , max(c.shop_nm_short) shop_nm
         , max(c.anal_dist_type_nm) anal_dist_type_nm
         , sum(a.sale_nml_qty + a.sale_ret_qty) sale_qty
         , sum(a.sale_nml_sale_amt + a.sale_ret_sale_amt) sale_amt
         , sum(a.sh_stock_qty) stock_qty
         , sum(ac_sale_nml_qty + ac_sale_ret_qty)                                     as ac_sale_qty
    from prcs.db_sh_scs_w_rpt a, prcs.dw_prdt b, prcs.db_shop c
    where a.prdt_cd = b.prdt_cd
    and a.brd_cd = c.brd_cd
    and a.shop_id = c.shop_id
    and b.brd_cd = '{brand}'
    and b.style_cd = '{product_cd}'
    and a.end_dt = '{end_date_this_week}'
    and c.mng_type = 'A'
    and c.anal_cntry  = 'KO'
    and c.shop_type = 'A'
    group by a.shop_id
    --having sum(a.sale_nml_sale_amt + a.sale_ret_sale_amt) != 0
    order by sum(a.sale_nml_sale_amt + a.sale_ret_sale_amt) desc
)a
order by sale_amt desc
        
        """

        query = get_query(
            query = shops_query,
            brand = brand,
            product_cd = product_cd,
            end_date_this_week = end_date_this_week,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        result = data.to_dict('records')

        return result

def get_query(query, *args, **kwargs):
    query = query.format(
        brand=kwargs["brand"],
        product_cd=kwargs["product_cd"],
        end_date_this_week=kwargs["end_date_this_week"],
    )
    return query
