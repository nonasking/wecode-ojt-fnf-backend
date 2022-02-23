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
from utils.check_item import check_keys_in_dictionary

class CategoryTreeView(View):

    #@connect_redshift
    def get(self, request, *args, **kwargs):
        return JsonResponse({
                "message": "test success",
                "categories": [
                    "가방",
                    "간절기아우터",
                    "기타용품",
                    "기타의류",
                    "데님",
                    "맨투맨",
                    "모자",
                    "스웨터",
                    "스윔웨어",
                    "신발",
                    "원피스",
                    "트레이닝셋업",
                    "티셔츠",
                    "패딩",
                    "팬츠",
                    "후리스"
                ],
                "subcategories": {
                    "TBA": [
                        "TBA"
                    ],
                    "가방": [
                        "기타가방",
                        "백팩",
                        "버킷백",
                        "숄더백",
                        "슬링백",
                        "크로스백",
                        "토트백",
                        "파우치",
                        "힙색"
                    ],
                    "간절기아우터": [
                        "기타아우터",
                        "바람막이",
                        "베이스볼점퍼"
                    ],
                    "기타용품": [
                        "기타",
                        "마스크",
                        "머플러",
                        "양말",
                        "장갑",
                        "쥬얼리",
                        "쿨토시",
                        "타올"
                    ],
                    "기타의류": [
                        "셔츠"
                    ],
                    "데님": [
                        "데님베스트",
                        "데님셔츠",
                        "데님스커트",
                        "데님자켓",
                        "데님팬츠"
                    ],
                    "맨투맨": [
                        "맨투맨",
                        "후드"
                    ],
                    "모자": [
                        "CP66",
                        "CP77",
                        "겨울소재",
                        "기타모",
                        "메쉬캡",
                        "방한모",
                        "베레모",
                        "볼캡",
                        "비니",
                        "스냅백",
                        "썬캡",
                        "코듀로이",
                        "쿨필드",
                        "테리",
                        "플리스",
                        "햇"
                    ],
                    "스웨터": [
                        "가디건",
                        "풀오버"
                    ],
                    "스윔웨어": [
                        "수영복_비키니",
                        "수영복_원피스",
                        "스윔팬츠"
                    ],
                    "신발": [
                        "뮬",
                        "빅볼청키도메인",
                        "빅볼청키오리진",
                        "샌들",
                        "슬리퍼",
                        "운동화",
                        "청키라이너",
                        "청키라이트",
                        "청키조거",
                        "청키클래식",
                        "청키하이",
                        "플레이볼"
                    ],
                    "원피스": [
                        "원피스"
                    ],
                    "트레이닝셋업": [
                        "집업",
                        "트레이닝팬츠"
                    ],
                    "티셔츠": [
                        "티셔츠",
                        "폴로티셔츠"
                    ],
                    "패딩": [
                        "경량패딩",
                        "다운점퍼",
                        "롱패딩",
                        "숏패딩",
                        "패딩베스트"
                    ],
                    "팬츠": [
                        "기타팬츠",
                        "레깅스",
                        "반바지",
                        "스커트",
                        "우븐팬츠",
                        "조거팬츠",
                        "카고팬츠",
                        "팬츠"
                    ],
                    "후리스": [
                        "후리스"
                    ]
                },
                "seasons": [
                    "22S",
                    "22N",
                    "22F",
                    "21S",
                    "21N",
                    "21F",
                    "20S",
                    "20F",
                    "19S",
                    "19F",
                    "18S",
                    "18F"
                ]
            })

        '''    
        try:
            required_keys = ["brand", "adult-kids"]
            check_keys_in_dictionary(request.GET, required_keys)
            
            brand = request.GET["brand"]
            adult_kids = request.GET["adult-kids"]
            connect =request.connect
            
            categories = self.get_categories(brand, adult_kids, connect)
            subcategories = self.get_subcategories(brand, adult_kids, connect)
            seasons = self.get_seasons(brand, adult_kids, connect)

            return JsonResponse({"message":"success", "categories":categories, "subcategories":subcategories, 'seasons':seasons}, status=200)
        
        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
        '''

    def get_query(self, query, *args, **kwargs):
        query = query.format(
            brand=kwargs["brand"],
            adult_kids=kwargs["adult_kids"],
        )
        return query
    
    def get_categories(self, brand, adult_kids, connect):
        categories_query = """
        
SELECT DISTINCT value
FROM (
         SELECT DISTINCT cat_nm AS value
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND adult_kids_nm = '{adult_kids}'
           AND cat_nm != 'TBA'
           AND ord_qty != 0
--         UNION ALL
--         SELECT DISTINCT cat_nm AS value
--         FROM prcs.db_srch_kwd_naver_mst
--         WHERE brd_cd = '{brand}'
--           AND adult_kids = '{adult_kids}'
--           AND comp_type != '라이프스타일'
--           AND cat_nm != '일반'
     ) a
ORDER BY 1        

        """
        query = self.get_query(
            query = categories_query,
            brand = brand,
            adult_kids = adult_kids,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data = data.to_dict('list')
        
        result = data['value']
                
        return result
    
    def get_subcategories(self, brand, adult_kids, connect):
        subcategories_query = """

SELECT DISTINCT sub_cat_nm AS value, cat_nm AS parent_value
FROM (
         SELECT DISTINCT cat_nm, sub_cat_nm
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND adult_kids_nm = '{adult_kids}'
           AND ord_qty != 0
--         UNION ALL
--         SELECT DISTINCT cat_nm, sub_cat_nm
--         FROM prcs.db_srch_kwd_naver_mst
--         WHERE brd_cd = '{brand}'
--           AND adult_kids = '{adult_kids}'
--           AND comp_type != '라이프스타일'
--           AND cat_nm != '일반'
     ) a
ORDER BY sub_cat_nm

        """
        query = self.get_query(
            query = subcategories_query,
            brand = brand,
            adult_kids = adult_kids,
        )
            
        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()
        
        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data = data.groupby('parent_value').agg({'value':lambda x: list(x)})
        data = data.to_dict()

        result = data['value']

        return result

    def get_seasons(self, brand, adult_kids, connect):
        seasons_query = """

SELECT a.*, row_number() OVER (order by value desc) AS id
FROM (
         SELECT DISTINCT trim(sesn) AS value
         FROM prcs.db_prdt
         WHERE brd_cd = '{brand}'
           AND ord_qty != 0
         ORDER BY 1 DESC
     ) a
ORDER BY id

        """
        query = self.get_query(
            query = seasons_query,
            brand = brand,
            adult_kids = adult_kids,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
        
        result = data['value'].tolist()

        return result

class StyleRankingTreeView(View):
    
    #@connect_redshift
    def get(self, request, *args, **kwargs):
        return JsonResponse({
                "message": "test success",
                "categories": [
                    {
                        "value": "pTBA",
                        "label": "TBA",
                        "children": [
                            {
                                "value": "TBA",
                                "label": "TBA"
                            }
                        ]
                    },
                    {
                        "value": "p가방",
                        "label": "가방",
                        "children": [
                            {
                                "value": "기타가방",
                                "label": "기타가방"
                            },
                            {
                                "value": "백팩",
                                "label": "백팩"
                            },
                            {
                                "value": "버킷백",
                                "label": "버킷백"
                            },
                            {
                                "value": "숄더백",
                                "label": "숄더백"
                            },
                            {
                                "value": "슬링백",
                                "label": "슬링백"
                            },
                            {
                                "value": "크로스백",
                                "label": "크로스백"
                            },
                            {
                                "value": "토트백",
                                "label": "토트백"
                            },
                            {
                                "value": "파우치",
                                "label": "파우치"
                            },
                            {
                                "value": "힙색",
                                "label": "힙색"
                            }
                        ]
                    },
                    {
                        "value": "p간절기아우터",
                        "label": "간절기아우터",
                        "children": [
                            {
                                "value": "기타아우터",
                                "label": "기타아우터"
                            },
                            {
                                "value": "바람막이",
                                "label": "바람막이"
                            },
                            {
                                "value": "베이스볼점퍼",
                                "label": "베이스볼점퍼"
                            }
                        ]
                    },
                    {
                        "value": "p기타용품",
                        "label": "기타용품",
                        "children": [
                            {
                                "value": "기타",
                                "label": "기타"
                            },
                            {
                                "value": "마스크",
                                "label": "마스크"
                            },
                            {
                                "value": "머플러",
                                "label": "머플러"
                            },
                            {
                                "value": "양말",
                                "label": "양말"
                            },
                            {
                                "value": "장갑",
                                "label": "장갑"
                            },
                            {
                                "value": "쥬얼리",
                                "label": "쥬얼리"
                            },
                            {
                                "value": "쿨토시",
                                "label": "쿨토시"
                            },
                            {
                                "value": "타올",
                                "label": "타올"
                            }
                        ]
                    },
                    {
                        "value": "p기타의류",
                        "label": "기타의류",
                        "children": [
                            {
                                "value": "셔츠",
                                "label": "셔츠"
                            }
                        ]
                    },
                    {
                        "value": "p데님",
                        "label": "데님",
                        "children": [
                            {
                                "value": "데님베스트",
                                "label": "데님베스트"
                            },
                            {
                                "value": "데님셔츠",
                                "label": "데님셔츠"
                            },
                            {
                                "value": "데님스커트",
                                "label": "데님스커트"
                            },
                            {
                                "value": "데님자켓",
                                "label": "데님자켓"
                            },
                            {
                                "value": "데님팬츠",
                                "label": "데님팬츠"
                            }
                        ]
                    },
                    {
                        "value": "p맨투맨",
                        "label": "맨투맨",
                        "children": [
                            {
                                "value": "맨투맨",
                                "label": "맨투맨"
                            },
                            {
                                "value": "후드",
                                "label": "후드"
                            }
                        ]
                    },
                    {
                        "value": "p모자",
                        "label": "모자",
                        "children": [
                            {
                                "value": "CP66",
                                "label": "CP66"
                            },
                            {
                                "value": "CP77",
                                "label": "CP77"
                            },
                            {
                                "value": "겨울소재",
                                "label": "겨울소재"
                            },
                            {
                                "value": "기타모",
                                "label": "기타모"
                            },
                            {
                                "value": "메쉬캡",
                                "label": "메쉬캡"
                            },
                            {
                                "value": "방한모",
                                "label": "방한모"
                            },
                            {
                                "value": "베레모",
                                "label": "베레모"
                            },
                            {
                                "value": "볼캡",
                                "label": "볼캡"
                            },
                            {
                                "value": "비니",
                                "label": "비니"
                            },
                            {
                                "value": "스냅백",
                                "label": "스냅백"
                            },
                            {
                                "value": "썬캡",
                                "label": "썬캡"
                            },
                            {
                                "value": "코듀로이",
                                "label": "코듀로이"
                            },
                            {
                                "value": "쿨필드",
                                "label": "쿨필드"
                            },
                            {
                                "value": "테리",
                                "label": "테리"
                            },
                            {
                                "value": "플리스",
                                "label": "플리스"
                            },
                            {
                                "value": "햇",
                                "label": "햇"
                            }
                        ]
                    },
                    {
                        "value": "p스웨터",
                        "label": "스웨터",
                        "children": [
                            {
                                "value": "가디건",
                                "label": "가디건"
                            },
                            {
                                "value": "풀오버",
                                "label": "풀오버"
                            }
                        ]
                    },
                    {
                        "value": "p스윔웨어",
                        "label": "스윔웨어",
                        "children": [
                            {
                                "value": "수영복_비키니",
                                "label": "수영복_비키니"
                            },
                            {
                                "value": "수영복_원피스",
                                "label": "수영복_원피스"
                            },
                            {
                                "value": "스윔팬츠",
                                "label": "스윔팬츠"
                            }
                        ]
                    },
                    {
                        "value": "p신발",
                        "label": "신발",
                        "children": [
                            {
                                "value": "뮬",
                                "label": "뮬"
                            },
                            {
                                "value": "빅볼청키도메인",
                                "label": "빅볼청키도메인"
                            },
                            {
                                "value": "빅볼청키오리진",
                                "label": "빅볼청키오리진"
                            },
                            {
                                "value": "샌들",
                                "label": "샌들"
                            },
                            {
                                "value": "슬리퍼",
                                "label": "슬리퍼"
                            },
                            {
                                "value": "운동화",
                                "label": "운동화"
                            },
                            {
                                "value": "청키라이너",
                                "label": "청키라이너"
                            },
                            {
                                "value": "청키라이트",
                                "label": "청키라이트"
                            },
                            {
                                "value": "청키조거",
                                "label": "청키조거"
                            },
                            {
                                "value": "청키클래식",
                                "label": "청키클래식"
                            },
                            {
                                "value": "청키하이",
                                "label": "청키하이"
                            },
                            {
                                "value": "플레이볼",
                                "label": "플레이볼"
                            }
                        ]
                    },
                    {
                        "value": "p원피스",
                        "label": "원피스",
                        "children": [
                            {
                                "value": "원피스",
                                "label": "원피스"
                            }
                        ]
                    },
                    {
                        "value": "p트레이닝셋업",
                        "label": "트레이닝셋업",
                        "children": [
                            {
                                "value": "집업",
                                "label": "집업"
                            },
                            {
                                "value": "트레이닝팬츠",
                                "label": "트레이닝팬츠"
                            }
                        ]
                    },
                    {
                        "value": "p티셔츠",
                        "label": "티셔츠",
                        "children": [
                            {
                                "value": "티셔츠",
                                "label": "티셔츠"
                            },
                            {
                                "value": "폴로티셔츠",
                                "label": "폴로티셔츠"
                            }
                        ]
                    },
                    {
                        "value": "p패딩",
                        "label": "패딩",
                        "children": [
                            {
                                "value": "경량패딩",
                                "label": "경량패딩"
                            },
                            {
                                "value": "다운점퍼",
                                "label": "다운점퍼"
                            },
                            {
                                "value": "롱패딩",
                                "label": "롱패딩"
                            },
                            {
                                "value": "숏패딩",
                                "label": "숏패딩"
                            },
                            {
                                "value": "패딩베스트",
                                "label": "패딩베스트"
                            }
                        ]
                    },
                    {
                        "value": "p팬츠",
                        "label": "팬츠",
                        "children": [
                            {
                                "value": "기타팬츠",
                                "label": "기타팬츠"
                            },
                            {
                                "value": "레깅스",
                                "label": "레깅스"
                            },
                            {
                                "value": "반바지",
                                "label": "반바지"
                            },
                            {
                                "value": "스커트",
                                "label": "스커트"
                            },
                            {
                                "value": "우븐팬츠",
                                "label": "우븐팬츠"
                            },
                            {
                                "value": "조거팬츠",
                                "label": "조거팬츠"
                            },
                            {
                                "value": "카고팬츠",
                                "label": "카고팬츠"
                            },
                            {
                                "value": "팬츠",
                                "label": "팬츠"
                            }
                        ]
                    },
                    {
                        "value": "p후리스",
                        "label": "후리스",
                        "children": [
                            {
                                "value": "후리스",
                                "label": "후리스"
                            }
                        ]
                    }
                ],
                "domains": [
                    {
                        "value": "ATHLEISURE",
                        "label": "ATHLEISURE"
                    },
                    {
                        "value": "BARK",
                        "label": "BARK"
                    },
                    {
                        "value": "BASIC",
                        "label": "BASIC"
                    },
                    {
                        "value": "CASHCOW",
                        "label": "CASHCOW"
                    },
                    {
                        "value": "CHECK",
                        "label": "CHECK"
                    },
                    {
                        "value": "CHECKERBOARD",
                        "label": "CHECKERBOARD"
                    },
                    {
                        "value": "COOLFIELD",
                        "label": "COOLFIELD"
                    },
                    {
                        "value": "CULSIVE",
                        "label": "CULSIVE"
                    },
                    {
                        "value": "CURSIVE",
                        "label": "CURSIVE"
                    },
                    {
                        "value": "ETHNICSTRIPE",
                        "label": "ETHNICSTRIPE"
                    },
                    {
                        "value": "FLORAL",
                        "label": "FLORAL"
                    },
                    {
                        "value": "GRAFFITI",
                        "label": "GRAFFITI"
                    },
                    {
                        "value": "HEART",
                        "label": "HEART"
                    },
                    {
                        "value": "ILLUSION",
                        "label": "ILLUSION"
                    },
                    {
                        "value": "LIKE",
                        "label": "LIKE"
                    },
                    {
                        "value": "MEGABEAR",
                        "label": "MEGABEAR"
                    },
                    {
                        "value": "MONOGRAM",
                        "label": "MONOGRAM"
                    },
                    {
                        "value": "MULTILOGO",
                        "label": "MULTILOGO"
                    },
                    {
                        "value": "PAISLEY",
                        "label": "PAISLEY"
                    },
                    {
                        "value": "PALMTREE",
                        "label": "PALMTREE"
                    },
                    {
                        "value": "PLAY",
                        "label": "PLAY"
                    },
                    {
                        "value": "PRIDETAG",
                        "label": "PRIDETAG"
                    },
                    {
                        "value": "RGB",
                        "label": "RGB"
                    },
                    {
                        "value": "SEAMBALL",
                        "label": "SEAMBALL"
                    },
                    {
                        "value": "SMILE",
                        "label": "SMILE"
                    },
                    {
                        "value": "SUMMERNIGHT",
                        "label": "SUMMERNIGHT"
                    },
                    {
                        "value": "SURROUND",
                        "label": "SURROUND"
                    },
                    {
                        "value": "TBA",
                        "label": "TBA"
                    },
                    {
                        "value": "THEYEAROFTIGER",
                        "label": "THEYEAROFTIGER"
                    },
                    {
                        "value": "TIE-DYE",
                        "label": "TIE-DYE"
                    },
                    {
                        "value": "WAPPEN",
                        "label": "WAPPEN"
                    }
                ],
                "seasons": [
                    {
                        "value": "pp18",
                        "label": "18",
                        "children": [
                            {
                                "value": "p18F",
                                "label": "18F",
                                "children": [
                                    {
                                        "value": "18F_Fall",
                                        "label": "18F_Fall"
                                    }
                                ]
                            },
                            {
                                "value": "p18S",
                                "label": "18S",
                                "children": [
                                    {
                                        "value": "18S_Spring",
                                        "label": "18S_Spring"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp19",
                        "label": "19",
                        "children": [
                            {
                                "value": "p19F",
                                "label": "19F",
                                "children": [
                                    {
                                        "value": "19F_Fall",
                                        "label": "19F_Fall"
                                    }
                                ]
                            },
                            {
                                "value": "p19S",
                                "label": "19S",
                                "children": [
                                    {
                                        "value": "19S_Spring",
                                        "label": "19S_Spring"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp20",
                        "label": "20",
                        "children": [
                            {
                                "value": "p20F",
                                "label": "20F",
                                "children": [
                                    {
                                        "value": "20F_Fall",
                                        "label": "20F_Fall"
                                    }
                                ]
                            },
                            {
                                "value": "p20S",
                                "label": "20S",
                                "children": [
                                    {
                                        "value": "20S_Spring",
                                        "label": "20S_Spring"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp21",
                        "label": "21",
                        "children": [
                            {
                                "value": "p21F",
                                "label": "21F",
                                "children": [
                                    {
                                        "value": "21F_Fall",
                                        "label": "21F_Fall"
                                    }
                                ]
                            },
                            {
                                "value": "p21N",
                                "label": "21N",
                                "children": [
                                    {
                                        "value": "21N_Non-Season",
                                        "label": "21N_Non-Season"
                                    }
                                ]
                            },
                            {
                                "value": "p21S",
                                "label": "21S",
                                "children": [
                                    {
                                        "value": "21S_Spring",
                                        "label": "21S_Spring"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp22",
                        "label": "22",
                        "children": [
                            {
                                "value": "p22F",
                                "label": "22F",
                                "children": [
                                    {
                                        "value": "22F_Fall",
                                        "label": "22F_Fall"
                                    }
                                ]
                            },
                            {
                                "value": "p22N",
                                "label": "22N",
                                "children": [
                                    {
                                        "value": "22N_Non-Season",
                                        "label": "22N_Non-Season"
                                    }
                                ]
                            },
                            {
                                "value": "p22S",
                                "label": "22S",
                                "children": [
                                    {
                                        "value": "22S_Spring",
                                        "label": "22S_Spring"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "ppX",
                        "label": "X",
                        "children": [
                            {
                                "value": "pX",
                                "label": "X",
                                "children": [
                                    {
                                        "value": "X_Non Season",
                                        "label": "X_Non Season"
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "items": [
                    {
                        "value": "ppACC",
                        "label": "ACC",
                        "children": [
                            {
                                "value": "pHeadwear",
                                "label": "Headwear",
                                "children": [
                                    {
                                        "value": "BN",
                                        "label": "BN"
                                    },
                                    {
                                        "value": "CB",
                                        "label": "CB"
                                    },
                                    {
                                        "value": "WM",
                                        "label": "WM"
                                    },
                                    {
                                        "value": "MC",
                                        "label": "MC"
                                    },
                                    {
                                        "value": "CP",
                                        "label": "CP"
                                    },
                                    {
                                        "value": "HT",
                                        "label": "HT"
                                    },
                                    {
                                        "value": "SC",
                                        "label": "SC"
                                    }
                                ]
                            },
                            {
                                "value": "pAcc_etc",
                                "label": "Acc_etc",
                                "children": [
                                    {
                                        "value": "MK",
                                        "label": "MK"
                                    },
                                    {
                                        "value": "GL",
                                        "label": "GL"
                                    },
                                    {
                                        "value": "JD",
                                        "label": "JD"
                                    },
                                    {
                                        "value": "SO",
                                        "label": "SO"
                                    },
                                    {
                                        "value": "JA",
                                        "label": "JA"
                                    },
                                    {
                                        "value": "PO",
                                        "label": "PO"
                                    },
                                    {
                                        "value": "TW",
                                        "label": "TW"
                                    },
                                    {
                                        "value": "ET",
                                        "label": "ET"
                                    },
                                    {
                                        "value": "MF",
                                        "label": "MF"
                                    },
                                    {
                                        "value": "JC",
                                        "label": "JC"
                                    },
                                    {
                                        "value": "JB",
                                        "label": "JB"
                                    },
                                    {
                                        "value": "ML",
                                        "label": "ML"
                                    }
                                ]
                            },
                            {
                                "value": "pBag",
                                "label": "Bag",
                                "children": [
                                    {
                                        "value": "OR",
                                        "label": "OR"
                                    },
                                    {
                                        "value": "BQ",
                                        "label": "BQ"
                                    },
                                    {
                                        "value": "BG",
                                        "label": "BG"
                                    },
                                    {
                                        "value": "BK",
                                        "label": "BK"
                                    },
                                    {
                                        "value": "CR",
                                        "label": "CR"
                                    },
                                    {
                                        "value": "BM",
                                        "label": "BM"
                                    },
                                    {
                                        "value": "HS",
                                        "label": "HS"
                                    }
                                ]
                            },
                            {
                                "value": "pShoes",
                                "label": "Shoes",
                                "children": [
                                    {
                                        "value": "SH",
                                        "label": "SH"
                                    },
                                    {
                                        "value": "SX",
                                        "label": "SX"
                                    },
                                    {
                                        "value": "MU",
                                        "label": "MU"
                                    },
                                    {
                                        "value": "SD",
                                        "label": "SD"
                                    },
                                    {
                                        "value": "LP",
                                        "label": "LP"
                                    },
                                    {
                                        "value": "CV",
                                        "label": "CV"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp의류",
                        "label": "의류",
                        "children": [
                            {
                                "value": "pOuter",
                                "label": "Outer",
                                "children": [
                                    {
                                        "value": "LE",
                                        "label": "LE"
                                    },
                                    {
                                        "value": "VT",
                                        "label": "VT"
                                    },
                                    {
                                        "value": "DV",
                                        "label": "DV"
                                    },
                                    {
                                        "value": "DK",
                                        "label": "DK"
                                    },
                                    {
                                        "value": "KC",
                                        "label": "KC"
                                    },
                                    {
                                        "value": "JK",
                                        "label": "JK"
                                    },
                                    {
                                        "value": "PD",
                                        "label": "PD"
                                    },
                                    {
                                        "value": "WJ",
                                        "label": "WJ"
                                    },
                                    {
                                        "value": "DJ",
                                        "label": "DJ"
                                    },
                                    {
                                        "value": "JP",
                                        "label": "JP"
                                    },
                                    {
                                        "value": "KT",
                                        "label": "KT"
                                    },
                                    {
                                        "value": "FD",
                                        "label": "FD"
                                    }
                                ]
                            },
                            {
                                "value": "pInner",
                                "label": "Inner",
                                "children": [
                                    {
                                        "value": "TS",
                                        "label": "TS"
                                    },
                                    {
                                        "value": "MT",
                                        "label": "MT"
                                    },
                                    {
                                        "value": "OP",
                                        "label": "OP"
                                    },
                                    {
                                        "value": "BS",
                                        "label": "BS"
                                    },
                                    {
                                        "value": "SW",
                                        "label": "SW"
                                    },
                                    {
                                        "value": "TR",
                                        "label": "TR"
                                    },
                                    {
                                        "value": "TK",
                                        "label": "TK"
                                    },
                                    {
                                        "value": "HD",
                                        "label": "HD"
                                    },
                                    {
                                        "value": "WS",
                                        "label": "WS"
                                    },
                                    {
                                        "value": "DR",
                                        "label": "DR"
                                    },
                                    {
                                        "value": "PQ",
                                        "label": "PQ"
                                    },
                                    {
                                        "value": "KP",
                                        "label": "KP"
                                    }
                                ]
                            },
                            {
                                "value": "pBottom",
                                "label": "Bottom",
                                "children": [
                                    {
                                        "value": "LG",
                                        "label": "LG"
                                    },
                                    {
                                        "value": "TP",
                                        "label": "TP"
                                    },
                                    {
                                        "value": "WP",
                                        "label": "WP"
                                    },
                                    {
                                        "value": "SM",
                                        "label": "SM"
                                    },
                                    {
                                        "value": "DS",
                                        "label": "DS"
                                    },
                                    {
                                        "value": "DP",
                                        "label": "DP"
                                    },
                                    {
                                        "value": "SK",
                                        "label": "SK"
                                    },
                                    {
                                        "value": "SP",
                                        "label": "SP"
                                    },
                                    {
                                        "value": "PT",
                                        "label": "PT"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "value": "pp저장품",
                        "label": "저장품",
                        "children": [
                            {
                                "value": "p저장품",
                                "label": "저장품",
                                "children": [
                                    {
                                        "value": "D",
                                        "label": "D"
                                    },
                                    {
                                        "value": "X",
                                        "label": "X"
                                    },
                                    {
                                        "value": "ZZ",
                                        "label": "ZZ"
                                    },
                                    {
                                        "value": "B",
                                        "label": "B"
                                    },
                                    {
                                        "value": "C",
                                        "label": "C"
                                    },
                                    {
                                        "value": "S",
                                        "label": "S"
                                    },
                                    {
                                        "value": "U",
                                        "label": "U"
                                    },
                                    {
                                        "value": "H",
                                        "label": "H"
                                    },
                                    {
                                        "value": "P",
                                        "label": "P"
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "adult_kids": [
                    {
                        "value": "성인",
                        "label": "성인"
                    }
                ]
            }) 

        '''    
        try:
            required_keys = ["brand"]
            check_keys_in_dictionary(request.GET, required_keys)
            
            brand = request.GET["brand"]
            connect =request.connect
            
            categories = self.get_categories(brand, connect)
            domains = self.get_domains(brand, connect)
            seasons = self.get_seasons(brand, connect)
            items = self.get_items(brand,connect)
            adult_kids = self.get_adult_kids(brand,connect)
            
            return JsonResponse({"message":"success", "categories":categories, 'domains':domains, 'seasons':seasons, 'items':items, 'adult_kids':adult_kids}, status=200)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, "message",str(e))}, status=400)
        '''
    
    def get_query(self, query, *args, **kwargs):
        query = query.format(
            brand=kwargs["brand"],
        )
        return query
    
    def get_categories(self, brand, connect):
        categories_query = """

select distinct cat_nm as category, sub_cat_nm as sub_category
from prcs.db_prdt
where brd_cd = '{brand}'
order by 1,2

        """
        query = self.get_query(
            query = categories_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data = data.groupby('category').agg({'sub_category':lambda x: list(x)})
        data = data.to_dict()
        
        result = data['sub_category']
        
        result = [{'value':'p'+key, 'label':key, 'children':result[key]} for key in result]

        for d in result:
            d['children'] = [{'value':v, 'label':v} for v in d['children']]

        return result
    
    def get_domains(self, brand, connect):
        domains_query = """

select case when domain1_nm is null then 'TBA' else domain1_nm end as domain
from (
         select distinct domain1_nm
         from prcs.db_prdt
         where brd_cd = '{brand}'
     ) a
order by 1

            """
        query = self.get_query(
            query = domains_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data['value'] = data['domain']
        data.columns = ['value', 'label']        
        
        result = data.to_dict('records')
        
        return result

    def get_seasons(self, brand, connect):
        seasons_query = """

select substring(sesn, 1, 2) as yy, sesn as season, sesn || '_' || sesn_sub_nm as subseason_nm
from (
         select distinct sesn, sesn_sub_nm
         from prcs.db_prdt
         where brd_cd = '{brand}'
     ) a
order by 1 desc, 2 desc, 3
            """
        query = self.get_query(
            query = seasons_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
        
        seasons_dict = {}
        for item in data.itertuples():
            if item[1] not in seasons_dict.keys():
                seasons_dict[item[1]] = {item[2]: [item[3]]}
            elif item[1] in seasons_dict.keys():
                if item[2] not in seasons_dict[item[1]].keys():
                    seasons_dict[item[1]][item[2]] = [item[3]]
                elif item[2] in seasons_dict[item[1]].keys():
                    seasons_dict[item[1]][item[2]] += [item[3]]
        
        result = [{
            'value': 'pp'+key,
            'label': key,
            'children': [{
                'value': 'p'+k,
                'label': k,
                'children': [{
                    'value': vv,
                    'label': vv
                } for vv in v]
            } for k,v in value.items()]
        }for key,value in seasons_dict.items()]

        return result

    def get_items(self, brand, connect):
        items_query = """

    select distinct parent_prdt_kind_nm, prdt_kind_nm, item
from prcs.db_prdt
where brd_cd = '{brand}'

        """
        query = self.get_query(
            query = items_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)
        
        items_dict = {}
        for item in data.itertuples():
            if item[1] not in items_dict.keys():
                items_dict[item[1]] = {item[2]: [item[3]]}
            elif item[1] in items_dict.keys():
                if item[2] not in items_dict[item[1]].keys():
                    items_dict[item[1]][item[2]] = [item[3]]
                elif item[2] in items_dict[item[1]].keys():
                    items_dict[item[1]][item[2]] += [item[3]]
        
        result = [{
            'value': 'pp'+key,
            'label': key,
            'children': [{
                'value': 'p'+k,
                'label': k,
                'children': [{
                    'value': vv,
                    'label': vv
                } for vv in v]
            } for k,v in value.items()]
        }for key,value in items_dict.items()]
        
        return result

    def get_adult_kids(self, brand, connect):
        adult_kids_query = """

select distinct adult_kids_nm as adult_kids_nm
from prcs.db_prdt
where brd_cd = '{brand}'
order by 1

        """
        query = self.get_query(
            query = adult_kids_query,
            brand = brand,
        )

        redshift_data = RedshiftData(connect, query)
        data = redshift_data.get_data()

        if data is None:
            return JsonResponse({"message":"QUERY_ERROR","query":query}, status=400)

        data['value'] = data['adult_kids_nm']
        data.columns = ['value', 'label'] 

        result = data.to_dict('records')
        
        return result