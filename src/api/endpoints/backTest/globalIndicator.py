import json
from fastapi import APIRouter, HTTPException
from src.model.portfolioModel import Portfolio
from src.utils.numpyEncoder import NumpyEncoder
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import date, timedelta, datetime

router = APIRouter()
client = AsyncIOMotorClient("mongodb://host.docker.internal:27017/")
db = client["snowball_data_engineer"]
etf_evaluate_collection = db["etf_evaluate"]
etf_information_collection = db["etf_information"]
etf_price_collection = db["etf_price"]
pdf_files_collection = db["pdf_files"]
stock_total_price = db["stock_total_price"]

# 화룡1(etfStockConsist - etf에 있는 주식 구성비율을 알려줌)
@router.get("/etfStockConsist")
async def etfStockConsist():
    pipeline = [
        {
            "$match": {
                "etf_code": "100910",
                "pdf_date": "20221201",
            },
        },
        {
            "$unwind": {
                "path": "$stock_list",
            },
        },
        {
            "$group": {
                "_id": None,
                "total_price": {
                    "$sum": "$stock_list.stock_total_price",
                },
                "stock_list": {
                    "$push": {
                        "stock_code": "$stock_list.stock_code",
                        "stock_name": "$stock_list.stock_name",
                        "stock_total_price": "$stock_list.stock_total_price",
                    },
                },
            },
        },
        {
            "$unwind": {
                "path": "$stock_list",
            },
        },
        {
            "$project": {
                "stock_code": "$stock_list.stock_code",
                "stock_name": "$stock_list.stock_name",
                "percentage_of_total_price": {
                    "$multiply": [
                        {
                            "$divide": [
                                "$stock_list.stock_total_price",
                                "$total_price",
                            ],
                        },
                        100,
                    ],
                },
            },
        },
    ]

    cursor = pdf_files_collection.aggregate(pipeline)  # Replace 'your_collection' with your collection name
    result = []
    async for document in cursor:
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result   
    

# 화룡함수2
@router.get("/marketEtfStockConsist")
async def etfStockConsist():
    pipeline = [
        {
            '$lookup': {
                'from': 'etf_information', 
                'localField': 'etf_code', 
                'foreignField': 'etf_code', 
                'as': 'etf_information'
            }
        }, {
            '$match': {
                'pdf_date': '20221201', 
                'etf_information.follow_multiple_category': '일반', 
                'etf_information.base_market_category': '국내', 
                'etf_information.base_asset_catogory': '주식'
            }
        }, {
            '$project': {
                'pdf_date': 1, 
                'etf_code': 1, 
                'stock_list': 1
            }
        }, {
            '$unwind': {
                'path': '$stock_list'
            }
        }, {
            '$match': {
                'stock_list.stock_total_price': {
                    '$ne': float('nan')
                }
            }
        }, {
            '$group': {
                '_id': '$etf_code', 
                'stock_kind_count': {
                    '$sum': 1
                }, 
                'total_price': {
                    '$sum': '$stock_list.stock_total_price'
                }, 
                'stock_list': {
                    '$push': {
                        'stock_code': '$stock_list.stock_code', 
                        'stock_name': '$stock_list.stock_name', 
                        'stock_total_price': '$stock_list.stock_total_price'
                    }
                }
            }
        }, {
            '$group': {
                '_id': None, 
                'etf_kind_count': {
                    '$sum': 1
                }, 
                'etf_list': {
                    '$push': {
                        'etf_code': '$_id', 
                        'etf_total_price': '$total_price', 
                        'stock_list': '$stock_list'
                    }
                }
            }
        }, {
            '$unwind': {
                'path': '$etf_list'
            }
        }, {
            '$unwind': {
                'path': '$etf_list.stock_list'
            }
        }, {
            '$project': {
                '_id': 0, 
                'etf_code_count': '$etf_code_count', 
                'etf_code': '$etf_list.etf_code', 
                'stock_code': '$etf_list.stock_list.stock_code', 
                'stock_name': '$etf_list.stock_list.stock_name', 
                'stock_ratio': {
                    '$multiply': [
                        {
                            '$divide': [
                                '$etf_list.stock_list.stock_total_price', '$etf_list.etf_total_price'
                            ]
                        }, 100
                    ]
                }, 
                'after_stock_ratio': {
                    '$divide': [
                        {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$etf_list.stock_list.stock_total_price', '$etf_list.etf_total_price'
                                    ]
                                }, 100
                            ]
                        }, '$etf_kind_count'
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$stock_code', 
                'stock_code': {
                    '$first': '$stock_code'
                }, 
                'stock_name': {
                    '$first': '$stock_name'
                }, 
                'market_stock_ratio': {
                    '$sum': '$after_stock_ratio'
                }
            }
        }, {
            '$sort': {
                'market_stock_ratio': -1
            }
        }
    ]

    cursor = pdf_files_collection.aggregate(pipeline)
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        result.append(document)
    return result 

# 화룡함수3
@router.get("/etfConfirmEvaluate")
async def etfConfirmEvaluate(gt:float = None, lt:float = None):
    
    match_stage = {
        'future_price_avg': {'$ne': None},
        'etf_price': {'$ne': None},
    }

    if lt is None:
        match_stage['etf_pbr'] = {'$gt': gt}
    else:
        match_stage['etf_pbr'] = {'$gt': gt, '$lt': lt}    
    
    pipeline =  [
        {
            '$group': {
                '_id': {
                    'etf_code': '$etf_code', 
                    'etf_year': '$etf_year_price', 
                    'etf_month': '$etf_month_price'
                }, 
                'etf_price_avg': {
                    '$avg': '$etf_price'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'etf_code': '$_id.etf_code', 
                'etf_year': '$_id.etf_year', 
                'etf_month': '$_id.etf_month', 
                'etf_price_avg': '$etf_price_avg'
            }
        }, {
            '$addFields': {
                'etf_date': {
                    '$dateFromString': {
                        'dateString': {
                            '$concat': [
                                {
                                    '$toString': '$etf_year'
                                }, '-', {
                                    '$toString': '$etf_month'
                                }, '-01'
                            ]
                        }, 
                        'format': '%Y-%m-%d'
                    }
                }
            }
        }, {
            '$addFields': {
                'before_date': {
                    '$dateSubtract': {
                        'startDate': '$etf_date', 
                        'unit': 'month', 
                        'amount': 1
                    }
                }
            }
        }, {
            '$addFields': {
                'before_year': {
                    '$year': '$before_date'
                }, 
                'before_month': {
                    '$month': '$before_date'
                }
            }
        }, {
            '$lookup': {
                'from': 'etf_evaluate', 
                'let': {
                    'field1Value': '$etf_code', 
                    'field2Value': '$before_year', 
                    'field3Value': '$before_month'
                }, 
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$etf_code', '$$field1Value'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$etf_year_price', '$$field2Value'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$etf_month_price', '$$field3Value'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result'
            }
        }, {
            '$project': {
                'etf_code': '$result.etf_code', 
                'future_price_avg': '$etf_price_avg', 
                'etf_cu': '$result.etf_cu', 
                'etf_price': '$result.etf_price', 
                'etf_date': '$result.etf_date', 
                'etf_year': '$result.etf_year_price', 
                'etf_month': '$result.etf_month_price', 
                'etf_day': '$result.etf_day_price', 
                'etf_per': '$result.etf_per', 
                'etf_pbr': '$result.etf_pbr', 
                'etf_psr': '$result.etf_psr', 
                'etf_operating_ratio': '$result.etf_operating_ratio', 
                'etf_profit_ratio': '$result.etf_profit_ratio', 
                'etf_debt_ratio': '$result.etf_debt_ratio', 
                'etf_roe': '$result.etf_roe'
            }
        }, {
            '$match': match_stage
        }, {
            '$project': {
                'etf_code': 1, 
                'etf_date': 1, 
                'change_rate': {
                    '$multiply': [
                        {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$future_price_avg', '$etf_price'
                                    ]
                                }, '$etf_price'
                            ]
                        }, 100
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$etf_date', 
                'future_change_rate_avg': {
                    '$avg': '$change_rate'
                }
            }
        },
        {
        "$sort":
            {
                "_id": 1,
            },
        }
    ]

    cursor = etf_evaluate_collection.aggregate(pipeline)
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        result.append(document)
    return result

# 화룡함수4
@router.get("/etfProgress")
async def etfProgress():
    pipeline = [
        {
            '$group': {
                '_id': '$start_year', 
                'year_etf_count': {
                    '$sum': 1
                }, 
                'etf_info': {
                    '$push': {
                        'base_asset_categories': '$base_asset_catogory'
                    }
                }
            }
        }, {
            '$unwind': {
                'path': '$etf_info'
            }
        }, {
            '$project': {
                '_id': 0, 
                'year': '$_id', 
                'year_etf_count': 1, 
                'base_asset_category': '$etf_info.base_asset_categories'
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$year', 
                    'base_asset_category': '$base_asset_category', 
                    'year_etf_count': '$year_etf_count'
                }, 
                'etf_category_count': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'year': '$_id.year', 
                'year_etf_count': '$_id.year_etf_count', 
                'asset_category': '$_id.base_asset_category', 
                'etf_category_count': '$etf_category_count'
            }
        }, {
            '$sort': {
                'year': 1
            }
        }
    ]
    
    cursor = etf_information_collection.aggregate(pipeline)
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        result.append(document)
    return result

# 태현함수1-1
@DeprecationWarning
@router.get("/MinValue")
async def getMinValue():
    # 쿼리 생성
    query = {
        'etf_code': '069500', # 특정 종목 코드 입력
        'etf_per': {'$gte': 0, '$lte': 30},
        'etf_pbr': {'$gte': 0, '$lte': 20},
        'etf_psr': {'$gte': 0, '$lte': 20},
        'etf_roe': {'$gte': -10, '$lte': 30},
        'etf_operating_ratio': {'$gte': 0, '$lte': 20},
        'etf_profit_ratio': {'$gte': 0, '$lte': 20},
        'etf_debt_ratio': {'$gte': 50},
    }

    # 쿼리 실행
    cursor = etf_evaluate_collection.aggregate([
        {'$match': query},
        {
            '$group': {
                '_id': None,
                'etf_per': {'$min': '$etf_per'},
                'etf_pbr': {'$min': '$etf_pbr'},
                'etf_psr': {'$min': '$etf_psr'},
                'etf_roe': {'$min': '$etf_roe'},
                'operating_ratio': {'$min': '$etf_operating_ratio'},
                'etf_profit_ratio': {'$min': '$etf_profit_ratio'},
                'etf_debt_ratio': {'$min': '$etf_debt_ratio'}
            }
        }
    ])
    
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result[0]
    
# 태현함수1-2
@DeprecationWarning
@router.get("/MaxValue")
async def getMaxValue():
    query = {
        'etf_code': '069500', # 특정 종목 코드 입력
        'etf_per': {'$gte': 0, '$lte': 30},
        'etf_pbr': {'$gte': 0, '$lte': 20},
        'etf_psr': {'$gte': 0, '$lte': 20},
        'etf_roe': {'$gte': -10, '$lte': 30},
        'etf_operating_ratio': {'$gte': 0, '$lte': 20},
        'etf_profit_ratio': {'$gte': 0, '$lte': 20},
        'etf_debt_ratio': {'$gte': 50},
    }

    # 쿼리 실행
    cursor = etf_evaluate_collection.aggregate([
        {'$match': query},
        {
            '$group': {
                '_id': None,
                'etf_per': {'$max': '$etf_per'},
                'etf_pbr': {'$max': '$etf_pbr'},
                'etf_psr': {'$max': '$etf_psr'},
                'etf_roe': {'$max': '$etf_roe'},
                'operating_ratio': {'$max': '$etf_operating_ratio'},
                'etf_profit_ratio': {'$max': '$etf_profit_ratio'},
                'etf_debt_ratio': {'$max': '$etf_debt_ratio'}
            }
        }
    ])

    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result[0]
    
# 태현함수1-3
@DeprecationWarning
@router.get('/recentValue')
async def getRecentValue():
    etf_code = '069500'
    query = {
        'etf_code': etf_code,
        'etf_per': {'$gte': 0, '$lte': 30},
        'etf_pbr': {'$gte': 0, '$lte': 20},
        'etf_psr': {'$gte': 0, '$lte': 20},
        'etf_roe': {'$gte': -10, '$lte': 30},
        'etf_operating_ratio': {'$gte': 0, '$lte': 20},
        'etf_profit_ratio': {'$gte': 0, '$lte': 20},
        'etf_debt_ratio': {'$gte': 50},
    }

    cursor = etf_evaluate_collection.find(query).sort('etf_date', -1).limit(1)
    
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        tempDoc = {}
        tempDoc["etf_per"] = document["etf_per"]
        tempDoc["etf_pbr"] = document["etf_pbr"]
        tempDoc["etf_psr"] = document["etf_psr"]
        tempDoc["etf_roe"] = document["etf_roe"]
        tempDoc["operating_ratio"] = document["etf_operating_ratio"]
        tempDoc["etf_profit_ratio"] = document["etf_profit_ratio"]
        tempDoc["etf_debt_ratio"] = document["etf_debt_ratio"]
        result.append(tempDoc)
    return result[0]
    
# 태현함수 2-1
@DeprecationWarning
@router.get("/MeanValue")
async def getMeanValue():
    query = {
        'etf_code': '100910', # 특정 종목 코드 입력
        'etf_per': {'$gte': 0, '$lte': 30},
        'etf_pbr': {'$gte': 0, '$lte': 20},
        'etf_psr': {'$gte': 0, '$lte': 20},
        'etf_roe': {'$gte': -10, '$lte': 30},
        'etf_operating_ratio': {'$gte': 0, '$lte': 20},
        'etf_profit_ratio': {'$gte': 0, '$lte': 20},
        'etf_debt_ratio': {'$gte': 50},
    }

    # 쿼리 실행
    cursor = etf_evaluate_collection.aggregate([
        {'$match': query},
        {'$sort': 
            {
                'etf_date':-1
            }
        },
        {
            '$group': {
                '_id': None,
                'etf_per': {'$avg': '$etf_per'},
                'etf_pbr': {'$avg': '$etf_pbr'},
                'etf_psr': {'$avg': '$etf_psr'},
                'etf_roe': {'$avg': '$etf_roe'},
                'operating_ratio': {'$avg': '$etf_operating_ratio'},
                'etf_profit_ratio': {'$avg': '$etf_profit_ratio'},
                'etf_debt_ratio': {'$avg': '$etf_debt_ratio'}
            }
        }
    ])

    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result[0]
    
# 태현함수2-2
@DeprecationWarning
@router.get("/TotalMeanValue")
async def getTotalMeanValue():
    query = {
        'etf_per': {'$gte': 0, '$lte': 30},
        'etf_pbr': {'$gte': 0, '$lte': 20},
        'etf_psr': {'$gte': 0, '$lte': 20},
        'etf_roe': {'$gte': -10, '$lte': 30},
        'etf_operating_ratio': {'$gte': 0, '$lte': 20},
        'etf_profit_ratio': {'$gte': 0, '$lte': 20},
        'etf_debt_ratio': {'$gte': 50}
    }

    # 쿼리 실행
    cursor = etf_evaluate_collection.aggregate([
        {'$match': query},
        {
            '$group': {
                '_id': None,
                'etf_per': {'$avg': '$etf_per'},
                'etf_pbr': {'$avg': '$etf_pbr'},
                'etf_psr': {'$avg': '$etf_psr'},
                'etf_roe': {'$avg': '$etf_roe'},
                'operating_ratio': {'$avg': '$etf_operating_ratio'},
                'etf_profit_ratio': {'$avg': '$etf_profit_ratio'},
                'etf_debt_ratio': {'$avg': '$etf_debt_ratio'}
            }
        }
    ])
    
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result[0]

@router.get("/MeanAndTotalMeanValue/{etfCode}")
async def getMeanAndTotalMeanValue(etfCode: str):
    product_date = datetime.strptime("2023-11-01", "%Y-%m-%d")
    
    pipeline = [
        {
            '$match': {
                'etf_date': product_date,
                'etf_per': {
                    '$gte': 0, 
                    '$lte': 30
                }, 
                'etf_pbr': {
                    '$gte': 0, 
                    '$lte': 20
                }, 
                'etf_psr': {
                    '$gte': 0, 
                    '$lte': 20
                }, 
                'etf_roe': {
                    '$gte': -10, 
                    '$lte': 30
                }, 
                'etf_operating_ratio': {
                    '$gte': 0, 
                    '$lte': 20
                }, 
                'etf_profit_ratio': {
                    '$gte': 0, 
                    '$lte': 20
                }, 
                'etf_debt_ratio': {
                    '$gte': 50
                }
            }
        }, {
            '$group': {
                '_id': None, # check
                'total_etf_per_avg': {
                    '$avg': '$etf_per'
                }, 
                'total_etf_pbr_avg': {
                    '$avg': '$etf_pbr'
                }, 
                'total_etf_psr_avg': {
                    '$avg': '$etf_psr'
                }, 
                'total_etf_roe_avg': {
                    '$avg': '$etf_roe'
                }, 
                'total_etf_operating_ratio_avg': {
                    '$avg': '$etf_operating_ratio'
                }, 
                'total_etf_profit_ratio_avg': {
                    '$avg': '$etf_profit_ratio'
                }, 
                'total_etf_debt_ratio_avg': {
                    '$avg': '$etf_debt_ratio'
                }
            }
        }, {
            '$addFields': {
                'match_etf_code': int(etfCode), # TODO 여기에 코드 넣으면 됨
                'match_etf_date': product_date
            }
        }, {
            '$lookup': {
                'from': 'etf_evaluate', 
                'let': {
                    'field1Value': '$match_etf_code', 
                    'field2Value': '$match_etf_date'
                }, 
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$etf_code', '$$field1Value'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$etf_date', '$$field2Value'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result'
            }
        }, {
            '$project': {
                'total_etf_per_avg': 1, 
                'total_etf_pbr_avg': 1, 
                'total_etf_psr_avg': 1, 
                'total_etf_roe_avg': 1, 
                'total_etf_operating_ratio_avg': 1, 
                'total_etf_profit_ratio_avg': 1, 
                'total_etf_debt_ratio_avg': 1, 
                'etf_per': '$result.etf_per', 
                'etf_pbr': '$result.etf_pbr', 
                'etf_psr': '$result.etf_psr', 
                'etf_roe': '$result.etf_roe', 
                'etf_operating_ratio': '$result.etf_operating_ratio', 
                'etf_profit_ratio': '$result.etf_profit_ratio', 
                'etf_debt_ratio': '$result.etf_debt_ratio'
            }
        }
    ]
    
    cursor = etf_evaluate_collection.aggregate(pipeline)
    print(cursor)
    result = []
    
    try:
        async for document in cursor:
            print(document)
            if "_id" in document:
                del document["_id"]
            result.append(document)
        return result[0]    
    except:
        result = {
                'total_etf_per_avg': 0, 
                'total_etf_pbr_avg': 0, 
                'total_etf_psr_avg': 0, 
                'total_etf_roe_avg': 0, 
                'total_etf_operating_ratio_avg': 0, 
                'total_etf_profit_ratio_avg': 0, 
                'total_etf_debt_ratio_avg': 0, 
                'etf_per': 0, 
                'etf_pbr': 0, 
                'etf_psr': 0,
                'etf_roe': 0, 
                'etf_operating_ratio': 0, 
                'etf_profit_ratio': 0, 
                'etf_debt_ratio': 0
            }
        return result
    
    

# 화룡's min max
@router.get("/minmax/{etfCode}")
async def getMinMax(etfCode: str):
    
    product_date = datetime.strptime("2023-11-01", "%Y-%m-%d")
    
    pipeline = [
        {
            '$match': {
                'etf_code': int(etfCode)
            }
        }, {
            '$group': {
                '_id': '$etf_code', 
                'etf_per_avg': {
                    '$avg': '$etf_per'
                }, 
                'etf_per_max': {
                    '$max': '$etf_per'
                }, 
                'etf_per_min': {
                    '$min': '$etf_per'
                }, 
                'etf_pbr_avg': {
                    '$avg': '$etf_pbr'
                }, 
                'etf_pbr_max': {
                    '$max': '$etf_pbr'
                }, 
                'etf_pbr_min': {
                    '$min': '$etf_pbr'
                }, 
                'etf_psr_avg': {
                    '$avg': '$etf_psr'
                }, 
                'etf_psr_max': {
                    '$max': '$etf_psr'
                }, 
                'etf_psr_min': {
                    '$min': '$etf_psr'
                }, 
                'etf_operating_ratio_avg': {
                    '$avg': '$etf_operating_ratio'
                }, 
                'etf_operating_ratio_max': {
                    '$max': '$etf_operating_ratio'
                }, 
                'etf_operating_ratio_min': {
                    '$min': '$etf_operating_ratio'
                }, 
                'etf_profit_ratio_avg': {
                    '$avg': '$etf_profit_ratio'
                }, 
                'etf_profit_ratio_max': {
                    '$max': '$etf_profit_ratio'
                }, 
                'etf_profit_ratio_min': {
                    '$min': '$etf_profit_ratio'
                }, 
                'etf_debt_ratio_avg': {
                    '$avg': '$etf_debt_ratio'
                }, 
                'etf_debt_ratio_max': {
                    '$max': '$etf_debt_ratio'
                }, 
                'etf_debt_ratio_min': {
                    '$min': '$etf_debt_ratio'
                }, 
                'etf_roe_avg': {
                    '$avg': '$etf_roe'
                }, 
                'etf_roe_max': {
                    '$max': '$etf_roe'
                }, 
                'etf_roe_min': {
                    '$min': '$etf_roe'
                }
            }
        }, {
            '$addFields': {
                'match_date': product_date
            }
        }, {
            '$lookup': {
                'from': 'etf_evaluate', 
                'let': {
                    'field1Value': '$_id', 
                    'field2Value': '$match_date'
                }, 
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$etf_code', '$$field1Value'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$etf_date', '$$field2Value'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ], 
                'as': 'result'
            }
        }, {
            '$unwind': {
                'path': '$result'
            }
        }, {
            '$project': {
                'etf_code': '$_id', 
                'etf_per_avg': 1, 
                'etf_per_max': 1, 
                'etf_per_min': 1, 
                'etf_pbr_avg': 1, 
                'etf_pbr_max': 1, 
                'etf_pbr_min': 1, 
                'etf_psr_avg': 1, 
                'etf_psr_max': 1, 
                'etf_psr_min': 1, 
                'etf_operating_ratio_avg': 1, 
                'etf_operating_ratio_max': 1, 
                'etf_operating_ratio_min': 1, 
                'etf_profit_ratio_avg': 1, 
                'etf_profit_ratio_max': 1, 
                'etf_profit_ratio_min': 1, 
                'etf_debt_ratio_avg': 1, 
                'etf_debt_ratio_max': 1, 
                'etf_debt_ratio_min': 1, 
                'etf_roe_avg': 1, 
                'etf_roe_max': 1, 
                'etf_roe_min': 1, 
                'etf_per': '$result.etf_per', 
                'etf_pbr': '$result.etf_pbr', 
                'etf_psr': '$result.etf_psr', 
                'etf_operating_ratio': '$result.etf_operating_ratio', 
                'etf_profit_ratio': '$result.etf_profit_ratio', 
                'etf_debt_ratio': '$result.etf_debt_ratio', 
                'etf_roe': '$result.etf_roe'
            }
        }
    ]
    
    cursor = etf_evaluate_collection.aggregate(pipeline)
    print(cursor)
    result = []
    
    try:
        async for document in cursor:
            print(document)
            if "_id" in document:
                del document["_id"]
            result.append(document)
        return result[0]
    except:
        result = {
            'etf_per_avg': 0.1, 
            'etf_per_max': 0.1, 
            'etf_per_min': 0.1, 
            'etf_pbr_avg': 0.1, 
            'etf_pbr_max': 0.1, 
            'etf_pbr_min': 0.1, 
            'etf_psr_avg': 0.1, 
            'etf_psr_max': 0.1, 
            'etf_psr_min': 0.1, 
            'etf_operating_ratio_avg': 0.1, 
            'etf_operating_ratio_max': 0.1, 
            'etf_operating_ratio_min': 0.1, 
            'etf_profit_ratio_avg': 0.1, 
            'etf_profit_ratio_max': 0.1, 
            'etf_profit_ratio_min': 0.1, 
            'etf_debt_ratio_avg': 0.1, 
            'etf_debt_ratio_max': 0.1, 
            'etf_debt_ratio_min': 0.1, 
            'etf_roe_avg': 0.1, 
            'etf_roe_max': 0.1, 
            'etf_roe_min': 0.1, 
            'etf_per': 0.1, 
            'etf_pbr': 0.1, 
            'etf_psr': 0.1, 
            'etf_operating_ratio': 0.1, 
            'etf_profit_ratio': 0.1, 
            'etf_debt_ratio': 0.1, 
            'etf_roe': 0.1
        }

@DeprecationWarning
@router.get("/etf_codes")
async def getEtfCode():
    pipeline = [
        {
            '$group': {
                '_id': "$etf_code"
            }
        },
        {
            '$project': {
                "etf_code": "$_id",
                "_id": 0 
            }
        }
    ]
    
    cursor = etf_evaluate_collection.aggregate(pipeline)
    print(cursor)
    result = []
    async for document in cursor:
        print(document)
        if "_id" in document:
            del document["_id"]
        result.append(document)
    return result