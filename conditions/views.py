import json
import psycopg2
import pandas as pd
import pandas.io.sql as psql

from django.http import JsonResponse
from django.views import View

from utils.connect_redshift import connect_redshift
from utils.redshift_data import RedshiftData
from utils.check_item import check_keys_in_dictionary
from utils.get_end_date_current_year import get_end_date_current_year

class SalesTrendView(View):
    @connect_redshift
    def get(self, request, type, *args, **kwargs):
        try:
            required_keys = ['brand', "product-cd"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            connect =request.connect

            end_date_current_year = get_end_date_current_year()
            '''
            if type in ['korea', 'global'] :
                result = self.get_sales_trend_data(brand,product_cd,end_date_current_year,type,connect)
            elif type == 'ratio':
                result = self.get_sales_trend_ratio_data(brand,product_cd,end_date_current_year,connect)
            '''

            if type == 'korea' :
                return JsonResponse({
                    "message": "test success",
                    "data": [
                        {
                            "end_dt": "21.12.05",
                            "qty_retail_cy": 1000,
                            "qty_dutyrfwhole_cy": 5000
                        },
                        {
                            "end_dt": "21.12.12",
                            "qty_retail_cy": 2000,
                            "qty_dutyrfwhole_cy": 10000
                        },
                        {
                            "end_dt": "21.12.19",
                            "qty_retail_cy": 3000,
                            "qty_dutyrfwhole_cy": 17000
                        },
                        {
                            "end_dt": "21.12.26",
                            "qty_retail_cy": 4000,
                            "qty_dutyrfwhole_cy": 18000
                        },
                        {
                            "end_dt": "22.01.02",
                            "qty_retail_cy": 5000,
                            "qty_dutyrfwhole_cy": 19000
                        },
                        {
                            "end_dt": "22.01.09",
                            "qty_retail_cy": 6000,
                            "qty_dutyrfwhole_cy": 25000
                        },
                        {
                            "end_dt": "22.01.16",
                            "qty_retail_cy": 7000,
                            "qty_dutyrfwhole_cy": 27000
                        },
                        {
                            "end_dt": "22.01.23",
                            "qty_retail_cy": 8000,
                            "qty_dutyrfwhole_cy": 30000
                        },
                        {
                            "end_dt": "22.01.30",
                            "qty_retail_cy": 9000,
                            "qty_dutyrfwhole_cy": 32000
                        },
                        {
                            "end_dt": "22.02.06",
                            "qty_retail_cy": 10000,
                            "qty_dutyrfwhole_cy": 33000
                        },
                        {
                            "end_dt": "22.02.13",
                            "qty_retail_cy": 11000,
                            "qty_dutyrfwhole_cy": 35000
                        },
                        {
                            "end_dt": "22.02.20",
                            "qty_retail_cy": 12000,
                            "qty_dutyrfwhole_cy": 40000
                        }
                    ]
                })
            
            elif type == 'global':
                return JsonResponse({
                    "message": "test success",
                    "data": [
                        {
                            "end_dt": "21.12.05",
                            "qty_kor_cy": 10000,
                            "qty_chn_cy": 1000,
                            "qty_gvl_cy": 2000
                        },
                        {
                            "end_dt": "21.12.12",
                            "qty_kor_cy": 20000,
                            "qty_chn_cy": 2000,
                            "qty_gvl_cy": 3000
                        },
                        {
                            "end_dt": "21.12.19",
                            "qty_kor_cy": 30000,
                            "qty_chn_cy": 3000,
                            "qty_gvl_cy": 4500
                        },
                        {
                            "end_dt": "21.12.26",
                            "qty_kor_cy": 35000,
                            "qty_chn_cy": 4000,
                            "qty_gvl_cy": 2000
                        },
                        {
                            "end_dt": "22.01.02",
                            "qty_kor_cy": 40000,
                            "qty_chn_cy": 5000,
                            "qty_gvl_cy": 7000
                        },
                        {
                            "end_dt": "22.01.09",
                            "qty_kor_cy": 50000,
                            "qty_chn_cy": 6000,
                            "qty_gvl_cy": 9000
                        },
                        {
                            "end_dt": "22.01.16",
                            "qty_kor_cy": 60000,
                            "qty_chn_cy": 7000,
                            "qty_gvl_cy": 1000
                        },
                        {
                            "end_dt": "22.01.23",
                            "qty_kor_cy": 70000,
                            "qty_chn_cy": 7500,
                            "qty_gvl_cy": 9500
                        },
                        {
                            "end_dt": "22.01.30",
                            "qty_kor_cy": 80000,
                            "qty_chn_cy": 8000,
                            "qty_gvl_cy": 10000
                        },
                        {
                            "end_dt": "22.02.06",
                            "qty_kor_cy": 85000,
                            "qty_chn_cy": 10000,
                            "qty_gvl_cy": 11000
                        },
                        {
                            "end_dt": "22.02.13",
                            "qty_kor_cy": 90000,
                            "qty_chn_cy": 15000,
                            "qty_gvl_cy": 12000
                        },
                        {
                            "end_dt": "22.02.20",
                            "qty_kor_cy": 100000,
                            "qty_chn_cy": 12222,
                            "qty_gvl_cy": 13000
                        }
                    ]
                })     
            
            elif type == 'ratio':
                return JsonResponse({
                    "message": "test success",
                    "data": [
                        {
                            "value": 223,
                            "name": "국내"
                        },
                        {
                            "value": 523,
                            "name": "면세/RF/도매"
                        }
                    ]
                })

            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_sales_trend_data(self, brand, product_cd, end_date_current_year, type, connect):
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
           and end_dt between '{end_date_current_year}' - 7 * 11 and '{end_date_current_year}'
     ) a
group by end_dt
order by end_dt asc

        """
        query = get_query(
            query = sales_trend_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_current_year = end_date_current_year,
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

    def get_sales_trend_ratio_data(self, brand, product_cd, end_date_current_year, connect):
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
           and end_dt = '{end_date_current_year}'
     ) a

        """
        query = get_query(
            query = sales_trend_ratio_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_current_year = end_date_current_year,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
                
        data.fillna(0, inplace=True)

        data = data.transpose()
        data.columns = ['value']
        data['name'] = ['국내', '면세/RF/도매']

        result = data.to_dict('records')

        return result

class WeeklyView(View):
    @connect_redshift
    def get(self, request, *args, **kwargs):
        try:
            required_keys = ['brand', "product-cd"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            connect =request.connect

            end_date_current_year = get_end_date_current_year()

            result = self.get_weekly_data(brand,product_cd,end_date_current_year,connect)

            return JsonResponse({
                "message": "test success",
                "data": [
                    {
                        "end_dt": "20220220",
                        "stor_qty_kor": 10000,
                        "delv_qty_exp": 1000,
                        "delv_qty_outlet": 1020,
                        "sale_qty_kor_ttl": 10372,
                        "sale_qty_kor_retail": 1,
                        "sale_qty_kor_duty": 101010,
                        "sale_qty_kor_rfwholesale": 28583,
                        "stock_qty_kor": 319482
                    },
                    {
                        "end_dt": "20220213",
                        "stor_qty_kor": 150,
                        "delv_qty_exp": 1500,
                        "delv_qty_outlet": 19,
                        "sale_qty_kor_ttl": 122,
                        "sale_qty_kor_retail": 2,
                        "sale_qty_kor_duty": 30201,
                        "sale_qty_kor_rfwholesale": 2444,
                        "stock_qty_kor": 19372
                    },
                    {
                        "end_dt": "20220206",
                        "stor_qty_kor": 2099,
                        "delv_qty_exp": 9000,
                        "delv_qty_outlet": 100,
                        "sale_qty_kor_ttl": 10101,
                        "sale_qty_kor_retail": 3,
                        "sale_qty_kor_duty": 24222,
                        "sale_qty_kor_rfwholesale": 193,
                        "stock_qty_kor": 13934
                    },
                    {
                        "end_dt": "20220130",
                        "stor_qty_kor": 1372,
                        "delv_qty_exp": 200,
                        "delv_qty_outlet": 34,
                        "sale_qty_kor_ttl": 20202,
                        "sale_qty_kor_retail": 40,
                        "sale_qty_kor_duty": 3989,
                        "sale_qty_kor_rfwholesale": 381,
                        "stock_qty_kor": 39828
                    },
                    {
                        "end_dt": "20220123",
                        "stor_qty_kor": 50000,
                        "delv_qty_exp": 501,
                        "delv_qty_outlet": 99,
                        "sale_qty_kor_ttl": 30303,
                        "sale_qty_kor_retail": 120,
                        "sale_qty_kor_duty": 3298,
                        "sale_qty_kor_rfwholesale": 1038,
                        "stock_qty_kor": 12894
                    }
                ]
            })

            #return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_weekly_data(self, brand, product_cd, end_date_current_year, connect):
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
and a.end_dt between '{end_date_current_year}'-7*12 and '{end_date_current_year}'
group by a.end_dt
order by a.end_dt desc
        
        """
        query = get_query(
            query = weekly_data_query,
            brand = brand,
            product_cd = product_cd,
            end_date_current_year = end_date_current_year,
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
            required_keys = ['brand', "product-cd"]
            check_keys_in_dictionary(request.GET, required_keys)

            brand = request.GET["brand"]
            product_cd = request.GET["product-cd"]
            connect =request.connect

            end_date_current_year = get_end_date_current_year()

            if type == 'overall':
                result = self.get_overall_data(brand,product_cd,end_date_current_year,connect)
                return JsonResponse({
                    "message": "test success",
                    "data": [
                        {
                            "type_zone_nm": "채널1",
                            "shop_cnt": 1000,
                            "asp": 100.0,
                            "sale_qty": 10000,
                            "shop_stock_qty": 15000,
                            "woi": 0.0,
                            "sale_amt": 111,
                            "ratio": 20.0,
                            "ac_sale_qty": 11111
                        },
                        {
                            "type_zone_nm": "채널2",
                            "shop_cnt": 2000,
                            "asp": 200.0,
                            "sale_qty": 20000,
                            "shop_stock_qty": 25000,
                            "woi": 3.0,
                            "sale_amt": 222,
                            "ratio": 20.0,
                            "ac_sale_qty": 22222
                        },
                        {
                            "type_zone_nm": "채널3",
                            "shop_cnt": 3000,
                            "asp": 300.0,
                            "sale_qty": 30000,
                            "shop_stock_qty": 35000,
                            "woi": 2.0,
                            "sale_amt": 333,
                            "ratio": 30.0,
                            "ac_sale_qty": 33333
                        },
                        {
                            "type_zone_nm": "채널4",
                            "shop_cnt": 4000,
                            "asp": 400.0,
                            "sale_qty": 40000,
                            "shop_stock_qty": 45000,
                            "woi": 9.0,
                            "sale_amt": 444,
                            "ratio": 10.0,
                            "ac_sale_qty": 44444
                        },
                        {
                            "type_zone_nm": "채널5",
                            "shop_cnt": 5000,
                            "asp": 500.0,
                            "sale_qty": 50000,
                            "shop_stock_qty": 55000,
                            "woi": 6.0,
                            "sale_amt": 555,
                            "ratio": 20.0,
                            "ac_sale_qty": 55555
                        }
                    ]
                })
            
            elif type == 'shops':
                result = self.get_shops_data(brand,product_cd,end_date_current_year,connect)
                return JsonResponse({
                    "message": "test success",
                    "data": [
                        {
                            "shopcode": "100",
                            "shop_nm": "매장1",
                            "anal_dist_type_nm": "채널1",
                            "asp": 1000.0,
                            "sale_qty": 10101,
                            "stock_qty": 11111,
                            "woi": 7.0,
                            "ac_sale_qty": 111111
                        },
                        {
                            "shopcode": "200",
                            "shop_nm": "매장2",
                            "anal_dist_type_nm": "채널2",
                            "asp": 2000.0,
                            "sale_qty": 20202,
                            "stock_qty": 22222,
                            "woi": 6.0,
                            "ac_sale_qty": 222222
                        },
                        {
                            "shopcode": "4",
                            "shop_nm": "매장3",
                            "anal_dist_type_nm": "채널3",
                            "asp": 3000.0,
                            "sale_qty": 30303,
                            "stock_qty": 33333,
                            "woi": 3.0,
                            "ac_sale_qty": 333333
                        },
                        {
                            "shopcode": "2",
                            "shop_nm": "매장4",
                            "anal_dist_type_nm": "채널4",
                            "asp": 4000.0,
                            "sale_qty": 40404,
                            "stock_qty": 44444,
                            "woi": 2.0,
                            "ac_sale_qty": 444444
                        },
                        {
                            "shopcode": "12",
                            "shop_nm": "매장5",
                            "anal_dist_type_nm": "채널5",
                            "asp": 5000.0,
                            "sale_qty": 50505,
                            "stock_qty": 55555,
                            "woi": 9.0,
                            "ac_sale_qty": 555555
                        }
                    ]
                })
            
            return JsonResponse({"message":"success", "data":result}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
    
    def get_overall_data(self, brand, product_cd, end_date_current_year, connect):
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
                    and a.end_dt = '{end_date_current_year}'
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
            end_date_current_year = end_date_current_year,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        result = data.to_dict('records')

        return result
    
    def get_shops_data(self, brand, product_cd, end_date_current_year, connect):
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
    and a.end_dt = '{end_date_current_year}'
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
            end_date_current_year = end_date_current_year,
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
        end_date_current_year=kwargs["end_date_current_year"],
    )

    return query
